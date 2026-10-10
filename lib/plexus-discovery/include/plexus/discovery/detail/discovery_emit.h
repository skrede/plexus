#ifndef HPP_GUARD_PLEXUS_DISCOVERY_DETAIL_DISCOVERY_EMIT_H
#define HPP_GUARD_PLEXUS_DISCOVERY_DETAIL_DISCOVERY_EMIT_H

#include "plexus/wire/announcement.h"

#include <cstdint>
#include <system_error>

namespace plexus::discovery::detail {

// Re-encode the cached announcement into the reused scratch (no per-emit alloc once warm) and
// send. Re-announces of the same (id, ep) are idempotent at note_peer (a map put), so no dedup.
template<typename D>
void emit_with_flags(D &d, std::uint8_t flags)
{
    if(!d.m_announcement)
        return;
    // The universe-pattern presence flag is a persistent property of this node's announcement (set
    // at build from the label); goodbye is the only transient per-emit bit. Preserve the former so a
    // re-announce or goodbye never drops the pattern and silently reverts to legacy-flagless wire.
    d.m_announcement->flags = flags | (d.m_announcement->flags & wire::k_announcement_universe_pattern_flag);
    wire::encode_announcement_into(d.m_scratch, *d.m_announcement);
    d.m_socket.send_multicast(d.m_scratch);
}

template<typename D>
void emit_announcement(D &d)
{
    emit_with_flags(d, 0);
}

// A best-effort single leave datagram: a browser evicts this node's awareness on receipt
// instead of waiting out the ttl.
template<typename D>
void emit_goodbye(D &d)
{
    emit_with_flags(d, wire::k_announcement_goodbye_flag);
}

template<typename D>
void arm_timer(D &d)
{
    d.m_timer.expires_after(d.m_jitter.next(d.m_options.announce_period));
    d.m_timer.async_wait(
            [&d](std::error_code ec)
            {
                if(ec || !d.m_advertising)
                    return;
                emit_announcement(d);
                arm_timer(d);
            });
}

}

#endif
