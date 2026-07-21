#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PEER_REPORT_CONSUMERS_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PEER_REPORT_CONSUMERS_H

#include "plexus/io/report_options.h"

#include "plexus/match/key_pattern.h"

#include "plexus/wire/peer_report.h"

#include "plexus/node_id.h"

#include <span>
#include <cstddef>

namespace plexus::io::detail {

// The origin-universe admission predicate, mirroring multicast_discovery::universe_admits but run
// against the report's ORIGIN universe (never the transport-sender's session): the concrete/concrete
// case keeps the uint32 fast-path (no parse, no alloc); otherwise the local and origin universe
// patterns must intersect. A malformed origin pattern or an unparsable local pattern fails closed.
inline bool report_universe_admits(const report_universe_ctx &local, const wire::peer_report &pr)
{
    if(local.is_concrete && !(pr.flags & wire::k_peer_report_universe_pattern_flag))
        return pr.origin_universe == local.universe;
    const auto peer = match::key_pattern::make(pr.origin_universe_pattern);
    return local.pattern && peer && local.pattern->intersects(*peer);
}

// The always-compiled receive seam: decode off the untrusted session frame and, only on a clean
// decode, hand the borrowed report to the engine fold. The gate chain (origin-universe, self, hop,
// dedup) runs engine-side, against the receiving node's own universe and route table — the reporter
// is the handshake-proven sender identity, distinct from the report's origin.
template<typename Forwarder>
void ingest_peer_report_frame(Forwarder &forwarder, const node_id &reporter, std::span<const std::byte> inner)
{
    if(auto pr = wire::decode_peer_report(inner))
        forwarder.note_peer_report(reporter, *pr);
}

}

#endif
