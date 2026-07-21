#ifndef HPP_GUARD_PLEXUS_IO_REPORT_OPTIONS_H
#define HPP_GUARD_PLEXUS_IO_REPORT_OPTIONS_H

#include "plexus/discovery/universe.h"

#include "plexus/match/key_pattern.h"
#include "plexus/match/detail/match_engine.h"

#include "plexus/wire/udp_dedup_window.h"

#include <string>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace plexus::io {

// The receiving node's own control-plane universe stamp plus the loop-safety bounds an inbound
// peer_report is admitted under. universe/universe_pattern mirror discovery_options: a node stamps
// here the same universe it advertises on, because the origin's universe rides the report but the
// RECEIVER's own is host-side config — no session field carries it (the pre-authorized config-stamp
// fallback). hop_budget defaults to depth-1 admission; dedup_depth sizes each candidate row's
// per-origin replay window. The numerics stay parametric (swept later), never pinned in a mechanism.
struct report_options
{
    std::uint32_t universe = discovery::k_default_universe;
    std::string universe_pattern;
    std::uint8_t hop_budget    = 1;
    std::size_t dedup_depth    = wire::udp_dedup_window::depth_max;
    // Flood bounds against a hostile authenticated relay: the distinct reported origins one reporter
    // may install, and the topic edges one reported origin may fold. Reject-and-count past either
    // ceiling; a direct peer is never affected. Numerics stay parametric (swept later), never pinned.
    std::size_t max_reported_origins = 256;
    std::size_t max_report_topics    = 256;
};

// The parse-once derivative the engine holds: the concrete uint32 fast-path key, whether the local
// universe is concrete, and the compiled pattern for the intersect path. A misconfigured local label
// fails closed (an empty pattern that admits nothing on the non-fast-path), exactly as the multicast
// local-parse does.
struct report_universe_ctx
{
    std::uint32_t universe = discovery::k_default_universe;
    bool is_concrete       = true;
    std::optional<match::key_pattern> pattern;
    // The canonical local label the relay stamps onto an emitted report when a pattern is configured,
    // so a pattern-universe node's report carries the pattern (and its presence flag) on the wire
    // instead of only the concrete uint32 the intersect path cannot match.
    std::string universe_pattern;
    std::uint8_t hop_budget          = 1;
    std::size_t dedup_depth          = wire::udp_dedup_window::depth_max;
    std::size_t max_reported_origins = 256;
    std::size_t max_report_topics    = 256;
};

inline report_universe_ctx make_report_ctx(const report_options &opts)
{
    report_universe_ctx ctx;
    ctx.universe             = opts.universe;
    ctx.hop_budget           = opts.hop_budget;
    ctx.dedup_depth          = opts.dedup_depth;
    ctx.max_reported_origins = opts.max_reported_origins;
    ctx.max_report_topics    = opts.max_report_topics;
    if(!opts.universe_pattern.empty())
    {
        ctx.universe_pattern = opts.universe_pattern;
        if(auto p = match::key_pattern::make(opts.universe_pattern))
        {
            ctx.pattern     = *p;
            ctx.is_concrete = match::detail::is_concrete(*p);
        }
        else
            ctx.is_concrete = false;
    }
    return ctx;
}

}

#endif
