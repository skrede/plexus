#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_HANDSHAKE_RESOLVE_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_HANDSHAKE_RESOLVE_H

#include "plexus/io/host_fingerprint.h"
#include "plexus/io/handshake_protocol.h"

#include "plexus/io/security/attach_facts.h"
#include "plexus/io/security/attach_policy.h"

#include "plexus/wire/handshake.h"

#include "plexus/node_id.h"

#include <cstdint>
#include <optional>

namespace plexus::io::detail {

template<typename F>
fsm_step_result abort_result(F &f, handshake_outcome reason) noexcept
{
    f.m_state = peer_fsm_state::not_connected;
    return {.action = fsm_action::abort, .outcome = reason};
}

template<typename F>
fsm_step_result reject_version_result(F &f) noexcept
{
    return abort_result(f, handshake_outcome::reject_version);
}

template<typename F>
fsm_step_result identity_conflict_result(F &f) noexcept
{
    return abort_result(f, handshake_outcome::reject_identity);
}

template<typename F>
fsm_step_result reject_unauthorized_result(F &f) noexcept
{
    return abort_result(f, handshake_outcome::reject_unauthorized);
}

template<typename F>
std::optional<fsm_step_result> validate(F &f, std::uint8_t peer_protocol_version, const node_id &peer_id, std::uint64_t peer_fingerprint,
                                        const security::attach_facts &facts = {}) noexcept
{
    f.m_peer_id          = peer_id;
    f.m_peer_fingerprint = host_fingerprint{peer_fingerprint};
    if(peer_protocol_version != wire::k_protocol_version)
        return reject_version_result(f);
    if(peer_id == f.m_cfg.self_id)
        return identity_conflict_result(f);
    if(f.m_cfg.attach_policy != nullptr && !f.m_cfg.attach_policy->decide(facts))
        return reject_unauthorized_result(f);
    return std::nullopt;
}

template<typename F>
bool racing_outbound(const F &f) noexcept
{
    return f.m_state == peer_fsm_state::handshaking || f.m_state == peer_fsm_state::handshake_resolved;
}

template<typename F>
fsm_step_result complete_inbound(F &f, dedup_decision dedup) noexcept
{
    f.m_state            = peer_fsm_state::handshake_resolved;
    f.m_complete_emitted = true;
    return {.action = fsm_action::complete, .outcome = handshake_outcome::accept_inbound, .dedup = dedup};
}

inline fsm_step_result accept_inbound_no_complete() noexcept
{
    return {.action = fsm_action::send_response, .outcome = handshake_outcome::accept_inbound};
}

// Greater node_id keeps its outbound; the loser keeps the inbound it accepted. The equal case is
// rejected at validation, so the comparison is strict and both sides agree on the survivor.
template<typename F>
dedup_decision arbitrate_dedup(const F &f) noexcept
{
    return f.m_cfg.self_id > f.m_peer_id ? dedup_decision::keep_outbound : dedup_decision::keep_inbound;
}

// A fresh inbound bootstrap completes inbound-only (no counter-dial to arbitrate). A mid-dial
// inbound with an outbound in flight is the second arrival of a simultaneous connect: arbitrate.
template<typename F>
fsm_step_result resolve_inbound(F &f, bool inbound_is_bootstrap) noexcept
{
    f.m_inbound_pending = true;
    if(racing_outbound(f))
        return f.m_complete_emitted ? accept_inbound_no_complete() : complete_inbound(f, arbitrate_dedup(f));
    if(inbound_is_bootstrap)
        return complete_inbound(f, dedup_decision::none);
    f.m_state = peer_fsm_state::handshake_resolved;
    return {.action = fsm_action::send_response, .outcome = handshake_outcome::accept_inbound};
}

template<typename F>
fsm_step_result resolve_outbound(F &f) noexcept
{
    if(f.m_state != peer_fsm_state::handshaking && f.m_state != peer_fsm_state::handshake_resolved)
        return {};
    if(f.m_complete_emitted)
        return {};
    auto dedup           = f.m_inbound_pending ? arbitrate_dedup(f) : dedup_decision::none;
    f.m_state            = peer_fsm_state::handshake_resolved;
    f.m_complete_emitted = true;
    return {.action = fsm_action::complete, .outcome = handshake_outcome::accept_outbound, .dedup = dedup};
}

template<typename F>
bool is_version_compatible(const F &f, std::uint8_t peer_major, std::uint8_t peer_minor) noexcept
{
    return (peer_major > f.m_cfg.compatible_version_major) || (peer_major == f.m_cfg.compatible_version_major && peer_minor >= f.m_cfg.compatible_version_minor);
}

}

#endif
