#ifndef HPP_GUARD_PLEXUS_IO_SHM_SHM_PREFERENCE_HOOK_H
#define HPP_GUARD_PLEXUS_IO_SHM_SHM_PREFERENCE_HOOK_H

#include "plexus/io/endpoint.h"
#include "plexus/io/multiplexing_transport.h"

#include "plexus/detail/compat.h"

#include <span>
#include <cstddef>
#include <utility>

namespace plexus::io {

// The same-medium preference hook: prefer the upgrade-capable candidate when the pair is
// upgrade-eligible AND the medium acquire succeeds, else fall back to the AF_UNIX candidate. This
// is the FIRST case where a tier (local) resolves to >1 candidate, and it is NOT a pure positional
// order: it depends on the RUNTIME acquire success (a forced broker failure must fall back to the
// stream). The hook reads the per-candidate local_fast_eligible flag, then consults the injected
// can_acquire probe (captured by move-only function so the erased hook stays decoupled from the
// concrete member type). The probe captures the member by reference; the member outlives the mux.
template<typename Member>
[[nodiscard]] inline io::selection_hook prefer_upgradeable_hook(Member &member)
{
    plexus::detail::move_only_function<bool(const endpoint &)> can_acquire =
            [&member](const endpoint &ep) { return member.can_acquire(ep); };
    return [probe = std::move(can_acquire)](
                   const endpoint                    &ep,
                   std::span<const io::mux_candidate> candidates) mutable -> std::size_t
    {
        std::size_t fallback      = candidates.front().index; // the first candidate (stream)
        bool        have_fallback = false;
        for(const io::mux_candidate &c : candidates)
        {
            if(c.local_fast_eligible)
            {
                if(probe(ep))
                    return c.index; // SHM-eligible AND the ring acquired: take the fast path
                continue;           // eligible but the acquire failed: keep scanning for a stream
            }
            if(!have_fallback)
            {
                fallback      = c.index; // the first non-SHM candidate is the stream fallback
                have_fallback = true;
            }
        }
        return fallback;
    };
}

}

#endif
