#ifndef HPP_GUARD_PLEXUS_TESTS_FUZZ_FUZZ_SESSION_LINK_H
#define HPP_GUARD_PLEXUS_TESTS_FUZZ_FUZZ_SESSION_LINK_H

#include "plexus/io/endpoint.h"
#include "plexus/io/peer_context.h"
#include "plexus/io/peer_session.h"
#include "plexus/io/lifecycle_event.h"
#include "plexus/io/message_forwarder.h"
#include "plexus/io/handshake_protocol.h"
#include "plexus/io/procedure_forwarder.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/log/logger.h"

#include "plexus/node_id.h"

#include <span>
#include <chrono>
#include <memory>
#include <string>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>

namespace fuzz_session {

using plexus::inproc::inproc_policy;
using session       = plexus::io::peer_session<inproc_policy>;
using channel       = plexus::inproc::inproc_channel<>;
using msg_forwarder = plexus::io::message_forwarder<inproc_policy>;
using rpc_forwarder = plexus::io::procedure_forwarder<inproc_policy>;

// The handshake timer runs on the steady clock, so a timeout this long never fires within one input and a run
// depends on the input alone.
constexpr std::chrono::hours k_handshake_timeout{1};
constexpr std::size_t k_drive_steps = 4096;

enum class side : std::uint8_t
{
    requester,
    responder
};

inline plexus::io::handshake_fsm_config make_cfg(std::uint8_t id_seed)
{
    plexus::node_id id{};
    id[0] = std::byte{id_seed};
    return {.self_id = id, .version_major = 1, .version_minor = 0, .compatible_version_major = 1, .compatible_version_minor = 0};
}

struct observation
{
    bool complete                 = false;
    bool torn_down                = false;
    std::uint32_t disconnects     = 0;
    std::uint64_t peer_session_id = 0;
};

struct session_end
{
    session_end(plexus::inproc::inproc_executor<> &ex, plexus::log::logger &sink)
            : ctx()
            , messages(sink)
            , procedures(ex, k_handshake_timeout, sink)
            , live()
            , seen()
    {
    }

    bool closed() noexcept
    {
        return live && live->pending().closing();
    }

    plexus::io::peer_context<inproc_policy> ctx;
    msg_forwarder messages;
    rpc_forwarder procedures;
    std::optional<session> live;
    observation seen;
};

// Two sessions stood up through the transport's listen/dial rendezvous: the dialed end is the requester and the
// accepted end the responder.
class session_link
{
public:
    session_link()
            : m_sink()
            , m_bus()
            , m_ex(m_bus)
            , m_transport(m_ex, m_bus)
            , m_requester(m_ex, m_sink)
            , m_responder(m_ex, m_sink)
    {
        m_transport.on_accepted([this](std::unique_ptr<channel> ch) { open(m_responder, std::move(ch), 0x01, 0x02, true); });
        m_transport.on_dialed([this](std::unique_ptr<channel> ch, const plexus::io::endpoint &) { open(m_requester, std::move(ch), 0x02, 0x01, false); });
        m_transport.listen({"inproc", "svc"});
        m_transport.dial({"inproc", "svc"});
    }

    session_end &at(side s) noexcept
    {
        return s == side::requester ? m_requester : m_responder;
    }

    bool drive()
    {
        for(std::size_t i = 0; i < k_drive_steps; ++i)
        {
            if(!m_ex.step())
                return true;
        }
        return false;
    }

    void inject(side s, std::span<const std::byte> frame)
    {
        if(auto &end = at(s); end.live)
            end.live->inject_receive(frame);
    }

    // An owner stops calling a session's verbs once it has torn it down.
    template<typename Verb>
    void call(side s, Verb verb)
    {
        if(auto &end = at(s); end.live && !end.seen.torn_down)
            verb(*end.live);
    }

    void tear_down(side s)
    {
        call(s,
             [&end = at(s)](session &live)
             {
                 end.seen.torn_down = true;
                 live.tear_down();
             });
    }

private:
    plexus::log::null_logger m_sink;
    plexus::inproc::inproc_bus<> m_bus;
    plexus::inproc::inproc_executor<> m_ex;
    plexus::inproc::inproc_transport<> m_transport;
    session_end m_requester;
    session_end m_responder;

    void open(session_end &end, std::unique_ptr<channel> ch, std::uint8_t self, std::uint8_t peer, bool inbound)
    {
        end.ctx.channel   = std::move(ch);
        end.ctx.node_name = inbound ? "requester-node" : "responder-node";
        end.ctx.peer_id   = make_cfg(peer).self_id;
        end.live.emplace(end.ctx, m_ex, make_cfg(self), k_handshake_timeout, end.messages, end.procedures, inbound, m_sink);
        end.live->on_lifecycle(
                [&end](const plexus::io::lifecycle_event &e)
                {
                    if(e.edge == plexus::io::lifecycle_edge::disconnected)
                        ++end.seen.disconnects;
                });
        end.live->start();
    }
};

}

#endif
