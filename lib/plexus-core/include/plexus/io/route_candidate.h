#ifndef HPP_GUARD_PLEXUS_IO_ROUTE_CANDIDATE_H
#define HPP_GUARD_PLEXUS_IO_ROUTE_CANDIDATE_H

#include "plexus/graph/participant_record.h"

#include "plexus/wire/udp_dedup_window.h"

#include <cstdint>

namespace plexus::io
{

struct route_candidate
{
    graph::route reach{};
    graph::provenance origin{};
    std::uint8_t hop{0};
    wire::udp_dedup_window seq_window{};
    std::uint64_t last_refreshed{0};

    // A nullopt via is the reserved direct-only state (participant_record.h), never a zero-node_id
    // sentinel; a relayed candidate always carries the relay hop.
    constexpr bool is_direct() const noexcept
    {
        return !reach.via.has_value();
    }
};

inline route_candidate direct_candidate(const endpoint &ep)
{
    return route_candidate{graph::route{ep, std::nullopt}, graph::provenance{graph::observation::directly_observed, std::nullopt}, 0, wire::udp_dedup_window{}, 0};
}

}

#endif
