#ifndef HPP_GUARD_TESTS_INTEGRATION_TRANSPORT_DIAL_UNIX_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_TRANSPORT_DIAL_UNIX_COMMON_H

#include "plexus/asio/unix_policy.h"
#include "plexus/asio/unix_transport.h"
#include "plexus/asio/unix_channel.h"

#include "plexus/io/peer_session.h"
#include "plexus/io/peer_context.h"
#include "plexus/io/transport_backend.h"
#include "plexus/io/message_forwarder.h"
#include "plexus/io/procedure_forwarder.h"

#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_bus.h"

#include "plexus/policy.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <unistd.h>

#include <span>
#include <chrono>
#include <memory>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <optional>
#include <string_view>

namespace pasio = plexus::asio;
namespace pio   = plexus::io;

using plexus::io::handshake_fsm_config;
using session       = pio::peer_session<pasio::unix_policy>;
using msg_forwarder = pio::message_forwarder<pasio::unix_policy>;
using rpc_forwarder = pio::procedure_forwarder<pasio::unix_policy>;

static_assert(plexus::io::transport_backend<pasio::unix_transport, pasio::unix_policy>);

namespace transport_dial_unix_fixture {

constexpr auto k_long_timeout = std::chrono::hours(1);

inline std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

inline std::string to_string(std::span<const std::byte> b)
{
    return std::string{reinterpret_cast<const char *>(b.data()), b.size()};
}

inline handshake_fsm_config make_cfg(std::uint8_t id_seed)
{
    plexus::node_id id{};
    id[0] = std::byte{id_seed};
    return handshake_fsm_config{.self_id = id, .version_major = 1, .version_minor = 0, .compatible_version_major = 1, .compatible_version_minor = 0};
}

// A per-instance owner-only temp directory + a SHORT socket path within it (well
// under the sun_path cap). The directory is removed on teardown after the socket
// file is unlinked (the listener unlinks it on stop, but rmdir needs an empty dir).
struct temp_sock
{
    std::string dir;
    std::string path;

    temp_sock()
    {
        char        tmpl[] = "/tmp/pxu-XXXXXX"; // short prefix keeps the full path well under sun_path
        const char *made   = ::mkdtemp(tmpl);
        dir                = made ? made : "";
        path               = dir + "/s";
    }

    ~temp_sock()
    {
        if(!path.empty())
            ::unlink(path.c_str());
        if(!dir.empty())
            ::rmdir(dir.c_str());
    }
};

// A DIALED AF_UNIX local-stream link turned into a peer_session pair — NO hand-dial.
// The transport's listen(ep) opens the acceptor on the socket path; dial(ep)
// async-connects, and the two ends arrive via on_accepted (the server end,
// is_inbound_bootstrap=true) and on_dialed (the client end,
// is_inbound_bootstrap=false). The sessions are deferred in std::optional and built
// only after the channels land. A real published message rides the same live local
// channels post-handshake. Each instance stands up a fresh transport + io_context +
// socket path so the looped iterations are independent.
struct dial_unix_link
{
    temp_sock             sock;
    ::asio::io_context    io;
    pasio::unix_transport transport{io};

    plexus::log::null_logger sink;
    msg_forwarder            req_messages{sink};
    msg_forwarder            resp_messages{sink};
    rpc_forwarder            req_procedures{io, k_long_timeout, sink};
    rpc_forwarder            resp_procedures{io, k_long_timeout, sink};

    plexus::io::peer_context<pasio::unix_policy> req_ctx;   // the dialer slot's per-peer record
    plexus::io::peer_context<pasio::unix_policy> resp_ctx;  // the accepted slot's per-peer record
    std::optional<session>                       requester; // the dialer (client) end
    std::optional<session>                       responder; // the accepted (server) end

    std::vector<std::string> req_received;
    std::vector<std::string> resp_received;

    explicit dial_unix_link(std::chrono::nanoseconds timeout = k_long_timeout)
    {
        transport.on_accepted(
                [this, timeout](std::unique_ptr<pasio::unix_channel> ch)
                {
                    resp_ctx.channel   = std::move(ch);
                    resp_ctx.node_name = "requester-node";
                    responder.emplace(resp_ctx, io, make_cfg(0x01), timeout, resp_messages, resp_procedures, true, sink);
                    responder->on_message([this](std::string_view, std::span<const std::byte> d) { resp_received.emplace_back(to_string(d)); });
                    responder->start();
                });
        transport.on_dialed(
                [this, timeout](std::unique_ptr<pasio::unix_channel> ch, const pio::endpoint &)
                {
                    req_ctx.channel   = std::move(ch);
                    req_ctx.node_name = "responder-node";
                    requester.emplace(req_ctx, io, make_cfg(0x02), timeout, req_messages, req_procedures, false, sink);
                    requester->on_message([this](std::string_view, std::span<const std::byte> d) { req_received.emplace_back(to_string(d)); });
                    requester->start();
                });

        transport.listen({"unix", sock.path});
        transport.dial({"unix", sock.path});
    }

    template<typename Pred>
    void pump_until(Pred pred)
    {
        auto bound = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while(!pred() && std::chrono::steady_clock::now() < bound)
            io.poll();
    }

    void settle(std::chrono::milliseconds window = std::chrono::milliseconds(20))
    {
        auto bound = std::chrono::steady_clock::now() + window;
        while(std::chrono::steady_clock::now() < bound)
            io.poll();
    }
};

}

#endif
