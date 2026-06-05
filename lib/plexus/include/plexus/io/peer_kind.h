#ifndef HPP_GUARD_PLEXUS_IO_PEER_KIND_H
#define HPP_GUARD_PLEXUS_IO_PEER_KIND_H

#include <cstdint>

namespace plexus::io {

// How this node came to hold a connection to a peer, carried on every lifecycle
// edge so an observer can reason about which edges a peer can fire. A `dialed`
// peer is one this node reached outbound: the redial driver owns it, so it can
// reconnect and ultimately surrender (dead). An `accepted` peer dialed US: the
// dialer owns the redial, so an accepted peer never fires reconnect or dead — it
// fires connected/disconnected/ready only. The source of truth is the session's
// is_inbound_bootstrap flag: inbound-bootstrap maps to accepted, outbound to dialed.
enum class peer_kind : std::uint8_t
{
    dialed,
    accepted
};

}

#endif
