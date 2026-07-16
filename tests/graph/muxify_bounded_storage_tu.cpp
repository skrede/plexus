// The muxify silent-heap-revert guard: a MULTI-transport bounded<> node must STILL resolve its
// engine's peer and topic tables to the fixed twins. muxify wraps only the mechanism policy, so a
// facade that read storage off engine_policy would revert to the heap here — these static_asserts
// fail the build if it does.

#include "plexus/node.h"

#include "plexus/inproc/inproc_policy.h"

#include "plexus/io/io_error.h"
#include "plexus/io/endpoint.h"
#include "plexus/io/fixed_peer_storage.h"

#include "plexus/graph/fixed_topic_storage.h"

#include "plexus/detail/compat.h"

#include <span>
#include <array>
#include <memory>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <type_traits>
#include <string_view>

namespace {

namespace pio = plexus::io;

// channel_adapter<C> requires the full byte_channel surface, so the member's channel_type must
// satisfy it even though this TU only names types and fires nothing.
struct dummy_channel
{
    pio::endpoint ep;

    void send(std::span<const std::byte>)
    {
    }
    void close()
    {
    }
    pio::endpoint remote_endpoint() const
    {
        return ep;
    }
    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)>)
    {
    }
    void on_closed(plexus::detail::move_only_function<void()>)
    {
    }
    void on_error(plexus::detail::move_only_function<void(pio::io_error)>)
    {
    }
    void on_protocol_close(plexus::detail::move_only_function<void(plexus::wire::close_cause)>)
    {
    }
    std::size_t backpressured() const noexcept
    {
        return 0;
    }
    std::uint64_t scheduler_key() const noexcept
    {
        return 1;
    }
};

static_assert(pio::byte_channel<dummy_channel>);

// A mux_member on the remote tier serving "tcp": listen/dial/close plus the four completion setters
// against its own channel, and the two static descriptors the multiplexer routes over.
template<int Id>
struct dummy_member
{
    using channel_type = dummy_channel;
    static constexpr std::array<std::string_view, 1> mux_schemes{"tcp"};
    static constexpr pio::transport_kind mux_tier = pio::transport_kind::remote;

    void listen(const pio::endpoint &)
    {
    }
    void dial(const pio::endpoint &)
    {
    }
    void close()
    {
    }
    void on_accepted(plexus::detail::move_only_function<void(std::unique_ptr<dummy_channel>)>)
    {
    }
    void on_dialed(plexus::detail::move_only_function<void(std::unique_ptr<dummy_channel>, const pio::endpoint &)>)
    {
    }
    void on_dial_failed(plexus::detail::move_only_function<void(const pio::endpoint &, pio::io_error)>)
    {
    }
    void on_error(plexus::detail::move_only_function<void(pio::io_error)>)
    {
    }
};

using policy       = plexus::inproc::inproc_policy;
using bounded_node = plexus::node<plexus::bounded<policy, 8, 16>, dummy_member<0>, dummy_member<1>>;
using engine       = bounded_node::engine_type;

static_assert(std::is_same_v<engine::peer_storage_type, plexus::io::fixed_peer_storage<8>>);
static_assert(std::is_same_v<engine::topic_storage_type, plexus::graph::fixed_topic_storage<16>>);

}

int main()
{
    return 0;
}
