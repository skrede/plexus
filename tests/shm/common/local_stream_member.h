#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_LOCAL_STREAM_MEMBER_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_LOCAL_STREAM_MEMBER_H

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/byte_channel.h"
#include "plexus/io/transport_selector.h"

#include "plexus/io/detail/scheduler_key.h"

#include "plexus/wire/close_cause.h"

#include "plexus/detail/compat.h"

#include <span>
#include <array>
#include <memory>
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace plexus::testing {

struct local_stream_channel
{
    plexus::io::endpoint ep;
    std::uint64_t key = plexus::io::detail::next_scheduler_key();
    void send(std::span<const std::byte>)
    {
    }
    void close()
    {
    }
    plexus::io::endpoint remote_endpoint() const
    {
        return ep;
    }
    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)>)
    {
    }
    void on_closed(plexus::detail::move_only_function<void()>)
    {
    }
    void on_error(plexus::detail::move_only_function<void(plexus::io::io_error)>)
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
        return key;
    }
};

static_assert(plexus::io::byte_channel<local_stream_channel>, "local_stream_channel must satisfy byte_channel");

// A local-tier stream member (the AF_UNIX stand-in) that serves "shm" too, so a same-host dial resolves to both it and
// a shared-memory member and the selection hook decides between them. It records whether the multiplexer routed the
// dial to it.
struct local_stream_member
{
    using channel_type = local_stream_channel;
    static constexpr std::array<std::string_view, 1> mux_schemes{"shm"};
    static constexpr plexus::io::transport_kind mux_tier = plexus::io::transport_kind::local;

    bool dialed = false;

    void listen(const plexus::io::endpoint &)
    {
    }
    void dial(const plexus::io::endpoint &)
    {
        dialed = true;
    }
    void close()
    {
    }
    void on_accepted(plexus::detail::move_only_function<void(std::unique_ptr<local_stream_channel>)>)
    {
    }
    void on_dialed(plexus::detail::move_only_function<void(std::unique_ptr<local_stream_channel>, const plexus::io::endpoint &)>)
    {
    }
    void on_dial_failed(plexus::detail::move_only_function<void(const plexus::io::endpoint &, plexus::io::io_error)>)
    {
    }
    void on_error(plexus::detail::move_only_function<void(plexus::io::io_error)>)
    {
    }
};

}

#endif
