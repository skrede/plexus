#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/transport_selector.h"
#include "plexus/io/polymorphic_byte_channel.h"
#include "plexus/io/multiplexing_transport.h"

#include "plexus/detail/compat.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <span>
#include <memory>
#include <string>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <string_view>

namespace pio = plexus::io;

namespace {

// A minimal byte_channel: it stores the seven verbs' callbacks and never fires them.
// channel_adapter<C> requires the full byte_channel surface, so the dummy member's
// channel_type must satisfy it even though this test never moves a real connection.
struct dummy_channel
{
    pio::endpoint ep;

    void                        send(std::span<const std::byte>) {}
    void                        close() {}
    [[nodiscard]] pio::endpoint remote_endpoint() const { return ep; }
    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)>) {}
    void on_closed(plexus::detail::move_only_function<void()>) {}
    void on_error(plexus::detail::move_only_function<void(plexus::io::io_error)>) {}
    void on_protocol_close(plexus::detail::move_only_function<void(plexus::wire::close_cause)>) {}
    [[nodiscard]] std::size_t   backpressured() const noexcept { return 0; }
    [[nodiscard]] std::uint64_t scheduler_key() const noexcept { return 1; }
};

static_assert(pio::byte_channel<dummy_channel>);

// A dummy member transport on the remote tier serving "tcp". It records whether its
// dial/listen was the one the multiplexer routed to — the substitutability probe. Two
// instances composed together are BOTH candidates for a "tcp" dial (same tier, same
// scheme), so the selection hook decides which one's dial fires.
template<int Id>
struct dummy_member
{
    using channel_type = dummy_channel;
    static constexpr std::array<std::string_view, 1> mux_schemes{"tcp"};
    static constexpr pio::transport_kind             mux_tier = pio::transport_kind::remote;

    bool dialed   = false;
    bool listened = false;

    void listen(const pio::endpoint &) { listened = true; }
    void dial(const pio::endpoint &) { dialed = true; }
    void close() {}

    void on_accepted(plexus::detail::move_only_function<void(std::unique_ptr<dummy_channel>)>) {}
    void on_dialed(plexus::detail::move_only_function<void(std::unique_ptr<dummy_channel>,
                                                           const pio::endpoint &)>)
    {
    }
    void on_dial_failed(
            plexus::detail::move_only_function<void(const pio::endpoint &, plexus::io::io_error)>)
    {
    }
    void on_error(plexus::detail::move_only_function<void(plexus::io::io_error)>) {}
};

using mux_t = pio::multiplexing_transport<dummy_member<0>, dummy_member<1>>;

}

TEST_CASE("mux_select_hook: the DEFAULT hook routes the dial to the FIRST candidate, looped",
          "[integration][mux][select][hook]")
{
    // The resolution is deterministic (no timing/transport), so the loop is a redundancy
    // check, not a flake hunt; the real reproducibility proof is re-running the process.
    constexpr int k_iterations = 100;
    int           completed    = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        // The members are borrowed by reference and MUST outlive the mux (non-movable).
        dummy_member<0> first;
        dummy_member<1> second;
        mux_t           mux{first, second};

        mux.dial({"tcp", "host:1"});

        // Both members are candidates (remote tier, "tcp" scheme); the default
        // first_candidate hook picks candidates.front() — the FIRST composed member.
        REQUIRE(first.dialed);
        REQUIRE_FALSE(second.dialed);
        ++completed;
    }
    REQUIRE(completed == k_iterations);
}

TEST_CASE("mux_select_hook: an INJECTED hook routes the dial to the SECOND candidate, looped",
          "[integration][mux][select][hook]")
{
    constexpr int k_iterations = 100;
    int           completed    = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        dummy_member<0> first;
        dummy_member<1> second;
        // The substitutability proof: an injected hook that returns the LAST candidate
        // index overrides the default — the mux must honor the erased callable, not a
        // hardcoded first-candidate choice. This is the behavioral substitutability the
        // multi-candidate selection seam requires.
        pio::selection_hook pick_last =
                [](const pio::endpoint &,
                   std::span<const pio::mux_candidate> candidates) -> std::size_t
        { return candidates.back().index; };
        mux_t mux{first, second, {}, std::move(pick_last)};

        mux.dial({"tcp", "host:1"});

        // The injected hook selected the SECOND candidate — the mux routed there, not to
        // the first. The default would have picked the first; the difference IS the proof.
        REQUIRE_FALSE(first.dialed);
        REQUIRE(second.dialed);
        ++completed;
    }
    REQUIRE(completed == k_iterations);
}

TEST_CASE("mux_select_hook: the injected hook also governs listen",
          "[integration][mux][select][hook]")
{
    dummy_member<0>     first;
    dummy_member<1>     second;
    pio::selection_hook pick_last =
            [](const pio::endpoint &, std::span<const pio::mux_candidate> candidates) -> std::size_t
    { return candidates.back().index; };
    mux_t mux{first, second, {}, std::move(pick_last)};

    mux.listen({"tcp", "host:1"});

    REQUIRE_FALSE(first.listened);
    REQUIRE(second.listened);
}
