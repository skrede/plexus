#ifndef HPP_GUARD_PLEXUS_IO_DROP_OBSERVER_H
#define HPP_GUARD_PLEXUS_IO_DROP_OBSERVER_H

#include "plexus/io/detail/drop_event.h"

namespace plexus::io {

// The public observer surface user code subclasses to learn why a frame was shed
// (an egress-band overflow or a receive-side datagram drop). It is the cold-path twin
// of peer_observer: a runtime-injected virtual interface registered on the routing
// engine, which fans every drop out POSTED on the Policy executor over a snapshot —
// NEVER synchronously from the drop site. That posted indirection is the DoS guard: an
// untrusted UDP flood drives the receive-side drop sites, and firing the observer inline
// per packet would amplify the flood into a synchronous-callback DoS. The single edge
// defaults to an empty body so an observer that only wants the always-on counters pays
// for no override. The drop_event is a by-value POD (scalars + node_id), so the posted
// turn carries a coalesced copy with no borrowed lifetime.
class drop_observer
{
public:
    virtual ~drop_observer() = default;

    virtual void on_drop(const io::detail::drop_event &) {}
};

}

#endif
