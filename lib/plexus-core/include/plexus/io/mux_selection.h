#ifndef HPP_GUARD_PLEXUS_IO_MUX_SELECTION_H
#define HPP_GUARD_PLEXUS_IO_MUX_SELECTION_H

#include "plexus/io/endpoint.h"

#include "plexus/detail/compat.h"

#include <span>
#include <cstddef>
#include <type_traits>

namespace plexus::io {

// A per-candidate eligibility descriptor the selection hook reads to pick one member when a tier
// resolves to N candidates: the member index + whether this candidate is the same-host fast-path.
struct mux_candidate
{
    std::size_t index        = 0;
    bool local_fast_eligible = false;
};

// Whether a member opts into the same-host fast path via `static constexpr bool
// mux_prefers_local_fast = true`. A trait, not a concept requirement, so every member stays valid.
template<typename M, typename = void>
struct member_prefers_local_fast : std::false_type
{
};

template<typename M>
struct member_prefers_local_fast<M, std::void_t<decltype(M::mux_prefers_local_fast)>> : std::bool_constant<M::mux_prefers_local_fast>
{
};

// The selection hook's erased type: when a tier resolves to N candidate members, the hook picks one
// by index over a span of per-candidate descriptors — NOT the members tuple, so it stays decoupled
// from the concrete member types. A cold-path dial/listen call, never the hot loop.
using selection_hook = plexus::detail::move_only_function<std::size_t(const endpoint &, std::span<const mux_candidate>)>;

struct first_candidate
{
    std::size_t operator()(const endpoint &, std::span<const mux_candidate> candidates) const noexcept
    {
        return candidates.front().index;
    }
};

}

#endif
