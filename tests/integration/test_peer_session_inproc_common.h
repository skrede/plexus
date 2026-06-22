#ifndef HPP_GUARD_TESTS_INTEGRATION_PEER_SESSION_INPROC_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_PEER_SESSION_INPROC_COMMON_H

#include "plexus/io/peer_session.h"
#include "plexus/io/peer_context.h"
#include "plexus/io/message_info.h"
#include "plexus/io/message_forwarder.h"
#include "plexus/io/procedure_forwarder.h"
#include "plexus/io/handshake_fsm.h"

#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_transport.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_bus.h"

#include "plexus/policy.h"

#include "plexus/wire/frame_codec.h"
#include "plexus/wire/frame.h"

#include <span>
#include <array>
#include <memory>
#include <chrono>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>
#include <string_view>
#include <system_error>

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_channel;
using plexus::inproc::inproc_transport;
using plexus::inproc::inproc_policy;
using plexus::io::handshake_fsm_config;
using plexus::wire::rpc_status;
using session       = plexus::io::peer_session<inproc_policy>;
using msg_forwarder = plexus::io::message_forwarder<inproc_policy>;
using rpc_forwarder = plexus::io::procedure_forwarder<inproc_policy>;

namespace peer_session_inproc_fixture {

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
    return handshake_fsm_config{.self_id                  = id,
                                .version_major            = 1,
                                .version_minor            = 0,
                                .compatible_version_major = 1,
                                .compatible_version_minor = 0};
}

// A two-node inproc link stood up through the transport's listen/dial rendezvous
// (no hand-dial): the dialer end becomes the requester (is_inbound_bootstrap=false)
// and the accepted end becomes the responder (is_inbound_bootstrap=true) — the
// realistic asymmetric handshake the B1 bridge completes on both sides. Each node
// owns its channel + forwarders + a peer_session for the single peer it talks to;
// the requester's request drives the responder to complete + answer, and the
// requester completes off that response. The channels are deferred in unique_ptr
// and the sessions in std::optional, built only once dial/on_accepted deliver the
// ends — and declared AFTER the bus/executor/transport so destruction unwinds the
// channels before the bus they registered on. Real messages and RPC ride the SAME
// live channels.
struct link
{
    inproc_bus<>       bus;
    inproc_executor<>  ex{bus};
    inproc_transport<> transport{ex, bus};

    plexus::log::null_logger sink;
    msg_forwarder req_messages{sink};
    msg_forwarder resp_messages{sink};
    rpc_forwarder req_procedures{ex, k_long_timeout};
    rpc_forwarder resp_procedures{ex, k_long_timeout};

    plexus::io::peer_context<inproc_policy> req_ctx;  // the dialer slot's per-peer record
    plexus::io::peer_context<inproc_policy> resp_ctx; // the accepted slot's per-peer record
    std::optional<session>                  requester;
    std::optional<session>                  responder;

    std::vector<std::string> req_received;
    std::vector<std::string> resp_received;

    explicit link(std::chrono::nanoseconds timeout = k_long_timeout)
    {
        transport.on_accepted(
                [this, timeout](std::unique_ptr<inproc_channel<>> ch)
                {
                    resp_ctx.channel   = std::move(ch);
                    resp_ctx.node_name = "requester-node";
                    // The reconciled peer identity (the registry sets this from the slot key; on a
                    // real accepter it is the post-inbound-reconciliation value). Pinned here so
                    // the source-identity gid reconstructs against the true peer node_id.
                    resp_ctx.peer_id = make_cfg(0x02).self_id; // the requester's node_id
                    responder.emplace(resp_ctx, ex, make_cfg(0x01), timeout, resp_messages,
                                      resp_procedures, true);
                    responder->on_message([this](std::string_view, std::span<const std::byte> d)
                                          { resp_received.emplace_back(to_string(d)); });
                    responder->start();
                });
        transport.on_dialed(
                [this, timeout](std::unique_ptr<inproc_channel<>> ch, const plexus::io::endpoint &)
                {
                    req_ctx.channel   = std::move(ch);
                    req_ctx.node_name = "responder-node";
                    req_ctx.peer_id =
                            make_cfg(0x01).self_id; // the responder's node_id (the dialed peer)
                    requester.emplace(req_ctx, ex, make_cfg(0x02), timeout, req_messages,
                                      req_procedures, false);
                    requester->on_message([this](std::string_view, std::span<const std::byte> d)
                                          { req_received.emplace_back(to_string(d)); });
                    requester->start();
                });

        transport.listen({"inproc", "svc"});
        transport.dial({"inproc", "svc"});
    }

    void drive() { ex.drain(); }
};

}

#endif
