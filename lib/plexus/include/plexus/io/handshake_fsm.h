#ifndef HPP_GUARD_PLEXUS_IO_HANDSHAKE_FSM_H
#define HPP_GUARD_PLEXUS_IO_HANDSHAKE_FSM_H

#include "plexus/wire/handshake.h"

#include "plexus/node_id.h"

#include <array>
#include <cstdint>
#include <optional>

namespace plexus::io {

// State the FSM advances through during a handshake. There is no `connected`
// enumerator: `connected` is bridge-owned, not an FSM state. handshake_resolved
// is the terminal state — the precondition the bridge composes onto.
enum class peer_fsm_state : std::uint8_t
{
    not_connected,
    dialing,
    handshaking,
    handshake_resolved
};

// Action the bridge must perform in response to an FSM step. The FSM is pure: it
// touches no socket, posts to no strand, schedules no timer, and moves no bytes.
// send_request / send_response name the two directions' intent; the bridge later
// encodes the matching frame.
enum class fsm_action : std::uint8_t
{
    none,
    send_request,
    send_response,
    complete,
    retry,
    abort
};

// Outcome of a handshake step. Drives bridge-side bookkeeping when a step accepts
// or rejects. reject_identity is the equal-node_id collision; reject_version
// covers both the exact-protocol gate and the compat-window failure.
enum class handshake_outcome : std::uint8_t
{
    none,
    accept_outbound,
    accept_inbound,
    reject_version,
    reject_identity
};

// Dedup arbitration verdict, populated only when a step completes with a pending
// counter-direction connection. The bridge owns the channel handles and drops the
// loser's; the FSM returns only the verdict.
enum class dedup_decision : std::uint8_t
{
    none,
    keep_outbound,
    keep_inbound
};

// Step result: the FSM's pure-data channel back to the bridge. No payload bytes —
// the codec is a later block, so "send_request"/"send_response" name intent only.
struct fsm_step_result
{
    fsm_action        action{fsm_action::none};
    handshake_outcome outcome{handshake_outcome::none};
    dedup_decision    dedup{dedup_decision::none};
};

// Static configuration for one handshake. node_id and the four self-version fields
// are required with NO default — a zeroed default would silently ship a real,
// comparable identity, so the categories stay distinct (required != default).
struct handshake_fsm_config
{
    node_id      self_id;
    std::uint8_t version_major;
    std::uint8_t version_minor;
    std::uint8_t compatible_version_major;
    std::uint8_t compatible_version_minor;
};

// Pure, sans-IO handshake state machine. Holds no asio / transport / logger types
// and moves no bytes — fully testable in isolation. The bridge feeds it via the
// six on_* events and reacts to the returned fsm_step_result. The FSM does NOT
// log: it returns abort/reject and the bridge logs at action-execution time.
class handshake_fsm
{
public:
    explicit handshake_fsm(const handshake_fsm_config &cfg) noexcept
        : m_cfg(cfg)
    {
    }

    // An outbound dial has begun. Subsequent on_timeout returns retry. Guarded to
    // the pre-handshake states: a stray dial once handshaking or resolved must NOT
    // regress an established or completed session back to dialing.
    fsm_step_result on_dial_started() noexcept
    {
        if(m_state == peer_fsm_state::not_connected || m_state == peer_fsm_state::dialing)
            m_state = peer_fsm_state::dialing;
        return {};
    }

    // The dial succeeded; we own an outbound connection. Emit send_request and
    // wait for the response to close the loop. Guarded so a stray connected event
    // on an already-handshaking or resolved session does not re-emit send_request.
    fsm_step_result on_outbound_connected() noexcept
    {
        if(m_state == peer_fsm_state::handshaking || m_state == peer_fsm_state::handshake_resolved)
            return {};
        m_state = peer_fsm_state::handshaking;
        return {.action = fsm_action::send_request};
    }

    // Inbound handshake_request. inbound_is_bootstrap marks a fresh inbound with
    // no counter-direction dial in flight (the common path under demand-driven
    // lazy dial): it completes inbound-only rather than stranding the session.
    fsm_step_result on_request(const wire::handshake_request &req, bool inbound_is_bootstrap) noexcept
    {
        m_last_seen_their_protocol_version = req.protocol_version;
        if(auto gate = validate(req.protocol_version, req.id))
            return *gate;
        if(!is_version_compatible(req.version_major, req.version_minor))
            return reject_version_result();
        return resolve_inbound(inbound_is_bootstrap);
    }

    // Inbound handshake_response. Compatible + accepted completes accept_outbound;
    // a rejected status or incompatible version aborts.
    fsm_step_result on_response(const wire::handshake_response &resp) noexcept
    {
        m_last_seen_their_protocol_version = resp.protocol_version;
        if(auto gate = validate(resp.protocol_version, resp.id))
            return *gate;
        if(resp.status == wire::handshake_status::identity_conflict)
            return identity_conflict_result();
        if(resp.status != wire::handshake_status::accepted)
            return reject_version_result();
        if(!is_version_compatible(resp.version_major, resp.version_minor))
            return reject_version_result();
        return resolve_outbound();
    }

    // The handshake timer fired. handshaking → abort; dialing → retry; else none.
    fsm_step_result on_timeout() noexcept
    {
        if(m_state == peer_fsm_state::handshaking)
        {
            m_state = peer_fsm_state::not_connected;
            return {.action = fsm_action::abort};
        }
        if(m_state == peer_fsm_state::dialing)
            return {.action = fsm_action::retry};
        return {};
    }

    // The bridge tore the peer down. Reset to a clean cycle: clear the latch and
    // the inbound-pending flag so a fresh handshake can complete again.
    fsm_step_result on_torn_down() noexcept
    {
        m_state = peer_fsm_state::not_connected;
        m_inbound_pending = false;
        m_complete_emitted = false;
        return {};
    }

    peer_fsm_state state() const noexcept { return m_state; }
    std::uint8_t last_seen_their_protocol_version() const noexcept { return m_last_seen_their_protocol_version; }

private:
    // The exact-match protocol gate runs ahead of every compat / status check, and
    // the identity-collision gate catches an equal node_id at validation so dedup
    // never sees the equal case. Returns a populated abort result when a gate trips;
    // captures the learned peer id for the dedup arbitration on the accept path.
    std::optional<fsm_step_result> validate(std::uint8_t peer_protocol_version, const node_id &peer_id) noexcept
    {
        m_peer_id = peer_id;
        if(peer_protocol_version != wire::k_protocol_version)
            return reject_version_result();
        if(peer_id == m_cfg.self_id)
            return identity_conflict_result();
        return std::nullopt;
    }

    // A fresh inbound bootstrap completes inbound-only (no counter-dial to
    // arbitrate). A mid-dial inbound with an outbound in flight is the second
    // arrival of a simultaneous connect: arbitrate and complete once. Otherwise
    // send the accept response and await the response side.
    fsm_step_result resolve_inbound(bool inbound_is_bootstrap) noexcept
    {
        m_inbound_pending = true;
        if(racing_outbound())
            return m_complete_emitted ? accept_inbound_no_complete() : complete_inbound(arbitrate_dedup());
        if(inbound_is_bootstrap)
            return complete_inbound(dedup_decision::none);
        m_state = peer_fsm_state::handshake_resolved;
        return {.action = fsm_action::send_response, .outcome = handshake_outcome::accept_inbound};
    }

    // The response that closes a single-direction dial, or the second arrival of a
    // simultaneous connect after on_request already completed (the latch no-ops it).
    // A response arriving with no outbound request ever sent (state not_connected /
    // dialing) is unsolicited: ignore it rather than fabricate a completion.
    fsm_step_result resolve_outbound() noexcept
    {
        if(m_state != peer_fsm_state::handshaking && m_state != peer_fsm_state::handshake_resolved)
            return {};
        if(m_complete_emitted)
            return {};
        auto dedup = m_inbound_pending ? arbitrate_dedup() : dedup_decision::none;
        m_state = peer_fsm_state::handshake_resolved;
        m_complete_emitted = true;
        return {.action = fsm_action::complete, .outcome = handshake_outcome::accept_outbound, .dedup = dedup};
    }

    bool racing_outbound() const noexcept
    {
        return m_state == peer_fsm_state::handshaking || m_state == peer_fsm_state::handshake_resolved;
    }

    fsm_step_result complete_inbound(dedup_decision dedup) noexcept
    {
        m_state = peer_fsm_state::handshake_resolved;
        m_complete_emitted = true;
        return {.action = fsm_action::complete, .outcome = handshake_outcome::accept_inbound, .dedup = dedup};
    }

    static fsm_step_result accept_inbound_no_complete() noexcept
    {
        return {.action = fsm_action::send_response, .outcome = handshake_outcome::accept_inbound};
    }

    // Greater node_id keeps its outbound; the loser keeps the inbound it accepted.
    // Both sides compute the same surviving connection (the equal case is rejected
    // at validation, so the comparison is strict).
    dedup_decision arbitrate_dedup() const noexcept
    {
        return m_cfg.self_id > m_peer_id ? dedup_decision::keep_outbound : dedup_decision::keep_inbound;
    }

    bool is_version_compatible(std::uint8_t peer_major, std::uint8_t peer_minor) const noexcept
    {
        return (peer_major > m_cfg.compatible_version_major)
            || (peer_major == m_cfg.compatible_version_major && peer_minor >= m_cfg.compatible_version_minor);
    }

    fsm_step_result reject_version_result() noexcept
    {
        m_state = peer_fsm_state::not_connected;
        return {.action = fsm_action::abort, .outcome = handshake_outcome::reject_version};
    }

    fsm_step_result identity_conflict_result() noexcept
    {
        m_state = peer_fsm_state::not_connected;
        return {.action = fsm_action::abort, .outcome = handshake_outcome::reject_identity};
    }

    handshake_fsm_config m_cfg;
    node_id              m_peer_id{};
    peer_fsm_state       m_state{peer_fsm_state::not_connected};
    std::uint8_t         m_last_seen_their_protocol_version{0};
    bool                 m_inbound_pending{false};
    // Latches true once a step has emitted complete. The matching second arrival
    // of a simultaneous connect skips re-emitting complete so the bridge installs
    // forwarders exactly once; on_torn_down clears it for a fresh cycle.
    bool                 m_complete_emitted{false};
};

}

#endif
