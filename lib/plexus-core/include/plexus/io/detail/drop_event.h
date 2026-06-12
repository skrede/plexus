#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_DROP_EVENT_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_DROP_EVENT_H

#include "plexus/io/locality.h"
#include "plexus/node_id.h"

#include <cstdint>

namespace plexus::io::detail {

// Why a frame was shed. The three egress-overflow causes mirror the per-band overflow
// verdicts (a saturated band evicting the oldest, refusing the newest, or refusing under
// block); `none` is the no-drop verdict an admitted frame reports. A future receive-side
// cause (a tamper/replay drop) extends this append-only — the event spine is shared.
enum class drop_cause : std::uint8_t
{
    none = 0,
    drop_oldest,
    drop_newest,
    blocked,
};

// A plain serializable drop record: cause, peer, topic, transport tier, band, and a
// coalescing count. It carries NO closure-with-context — it is an aggregate of
// scalars so it timestamps, serializes, and multiplexes onto a future unified event spine
// without preclusion. `band` is meaningful for egress causes only (a receive-side cause
// leaves it 0). `count` coalesces a burst of identical drops into one record. The emission
// POINTS land at the reshaped overflow edges now; the consuming observer hook wires in
// later (the null-default observer shape, one predictable branch when unused).
struct drop_event
{
    drop_cause   cause{drop_cause::none};
    locality     transport{locality::any};   // the delivery tier the dropped frame targeted
    std::uint8_t band{0};                     // the egress priority band (egress causes only)
    std::uint64_t topic_hash{0};
    node_id      peer{};                      // the subscribed peer's node identity
    std::uint64_t count{1};                   // burst-coalescing count
};

}

#endif
