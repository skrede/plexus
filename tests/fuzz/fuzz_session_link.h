#ifndef HPP_GUARD_PLEXUS_TESTS_FUZZ_FUZZ_SESSION_LINK_H
#define HPP_GUARD_PLEXUS_TESTS_FUZZ_FUZZ_SESSION_LINK_H

#include "fuzz_oracle.h"
#include "fuzz_session_policy.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/peer_context.h"
#include "plexus/io/peer_session.h"
#include "plexus/io/lifecycle_event.h"
#include "plexus/io/handshake_protocol.h"
#include "plexus/io/subscribe_retry_config.h"

#include "plexus/log/logger.h"

#include "plexus/node_id.h"

#include <span>
#include <chrono>
#include <memory>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>
#include <string_view>

namespace fuzz_session {

// Time moves only when the input advances fuzz_clock, so this window, like the subscribe retry, expires only where
// the input says.
constexpr std::chrono::seconds k_handshake_timeout{1};
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
    std::optional<plexus::node_id> identity;
};

struct counting_logger final : plexus::log::logger
{
    counting_logger() noexcept
            : lines(0)
    {
    }

    void warn(std::string_view) override
    {
        ++lines;
    }

    std::uint64_t lines;
};

struct session_end
{
    session_end(executor &ex, plexus::log::logger &sink)
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

    plexus::io::peer_context<fuzz_policy> ctx;
    msg_forwarder messages;
    rpc_forwarder procedures;
    std::optional<session> live;
    observation seen;
};

inline void note_edge(session_end &end, plexus::io::lifecycle_edge edge)
{
    if(edge == plexus::io::lifecycle_edge::disconnected)
        ++end.seen.disconnects;
    if(edge == plexus::io::lifecycle_edge::connected && !end.seen.identity)
        end.seen.identity = end.live->peer_identity();
}

// The establishment gate admits nothing to a consumer before completion, so no consumer callback runs on an
// incomplete session.
inline void observe(session_end &end)
{
    end.live->on_lifecycle([&end](const plexus::io::lifecycle_event &e) { note_edge(end, e.edge); });
    end.live->on_message([&end](std::string_view, std::span<const std::byte>) { fuzz_require(end.live->is_complete(), "on_message runs only on a complete session"); });
    end.live->on_stamp_seen([&end](const plexus::node_id &) { fuzz_require(end.live->is_complete(), "on_stamp_seen runs only on a complete session"); });
}

// Two sessions stood up through the transport's listen/dial rendezvous: the dialed end is the requester and the
// accepted end the responder.
class session_link
{
public:
    session_link()
            : m_bus()
            , m_ex(m_bus)
            , m_transport(m_ex, m_bus)
            , m_sink()
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
    bus m_bus;
    executor m_ex;
    transport m_transport;
    counting_logger m_sink;
    session_end m_requester;
    session_end m_responder;

    void open(session_end &end, std::unique_ptr<channel> ch, std::uint8_t self, std::uint8_t peer, bool inbound)
    {
        end.ctx.channel   = std::move(ch);
        end.ctx.node_name = inbound ? "requester-node" : "responder-node";
        end.ctx.peer_id   = make_cfg(peer).self_id;
        end.live.emplace(end.ctx, m_ex, make_cfg(self), k_handshake_timeout, end.messages, end.procedures, inbound, m_sink);
        end.live->set_subscribe_retry(plexus::io::subscribe_retry_config{});
        observe(end);
        end.live->start();
    }
};

}

#endif
