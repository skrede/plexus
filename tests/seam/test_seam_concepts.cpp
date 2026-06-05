// Seam-concept smoke test: compile-time proof that the byte_channel / Policy
// concepts are satisfiable, plus the one-line-diagnostic affordance and the
// wire_bytes owner-keeps-bytes-alive guarantee. This is the SLICE-4
// maintainability evidence — the concept surface rejects a non-conforming
// backend with a single static_assert line, not a template-instantiation dump.

#include "plexus/policy.h"
#include "plexus/io/byte_channel.h"
#include "plexus/wire_bytes.h"
#include "plexus/detail/compat.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <vector>
#include <chrono>
#include <cstddef>
#include <memory>
#include <system_error>

using plexus::detail::move_only_function;

namespace {

// A minimal type satisfying byte_channel: every member the concept names, and
// nothing welded to a networking backend.
struct fake_channel
{
    fake_channel() = default;
    explicit fake_channel(int) {}
    fake_channel(int, std::error_code &) {}

    void send(std::span<const std::byte>) {}
    void close() {}
    plexus::io::endpoint remote_endpoint() const { return {}; }
    void on_data(move_only_function<void(std::span<const std::byte>)>) {}
    void on_closed(move_only_function<void()>) {}
    void on_error(move_only_function<void(plexus::io::io_error)>) {}
    void on_protocol_close(move_only_function<void(plexus::wire::close_cause)>) {}
};

struct fake_timer
{
    explicit fake_timer(int) {}
    fake_timer(int, std::error_code &) {}

    void expires_after(std::chrono::milliseconds) {}
    void async_wait(move_only_function<void(std::error_code)>) {}
    void cancel() {}
};

// A minimal Policy over the fakes: executor is a plain int handle the channel
// and timer construct from, byte_owner is the FORK-M default.
struct fake_policy
{
    using executor_type = int;
    using byte_channel_type = fake_channel;
    using timer_type = fake_timer;
    using byte_owner = std::shared_ptr<const void>;

    static void post(executor_type, move_only_function<void()>) {}
};

static_assert(plexus::io::byte_channel<fake_channel>,
              "fake_channel must satisfy byte_channel");
static_assert(plexus::Policy<fake_policy>,
              "fake_policy must satisfy Policy");

#if 0
// Flip to 1 to observe the one-line concept diagnostic. broken_channel drops
// send(), so static_assert(byte_channel<broken_channel>) fails pointing at the
// offending member — not a 50-frame template-instantiation dump.
struct broken_channel
{
    void close() {}
    plexus::io::endpoint remote_endpoint() const { return {}; }
    void on_data(move_only_function<void(std::span<const std::byte>)>) {}
    void on_closed(move_only_function<void()>) {}
    void on_error(move_only_function<void(plexus::io::io_error)>) {}
    void on_protocol_close(move_only_function<void(plexus::wire::close_cause)>) {}
};
static_assert(plexus::io::byte_channel<broken_channel>,
              "broken_channel intentionally fails: send() is missing");
#endif

}

TEST_CASE("wire_bytes owner keeps the bytes alive past the source scope", "[seam]")
{
    const std::byte *aliased = nullptr;
    plexus::wire_bytes<> wb;

    {
        plexus::wire::shared_bytes source{
            std::vector<std::byte>{std::byte{0xDE}, std::byte{0xAD}, std::byte{0xBE}, std::byte{0xEF}}};
        aliased = source.data();
        wb = plexus::wire_bytes<>{source};

        REQUIRE(wb.size() == 4);
        REQUIRE(wb.data() == aliased); // view aliases the source's buffer
    }

    // source is gone; the owner handle inside wb must keep the bytes alive.
    REQUIRE(wb.size() == 4);
    REQUIRE(wb.data() == aliased);
    REQUIRE(wb.owner() != nullptr);
    REQUIRE(static_cast<std::byte>(wb.data()[0]) == std::byte{0xDE});
    REQUIRE(static_cast<std::byte>(wb.data()[3]) == std::byte{0xEF});
}
