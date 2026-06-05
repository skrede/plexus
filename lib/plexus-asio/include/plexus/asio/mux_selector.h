#ifndef HPP_GUARD_PLEXUS_ASIO_MUX_SELECTOR_H
#define HPP_GUARD_PLEXUS_ASIO_MUX_SELECTOR_H

#include "plexus/io/endpoint.h"

#include <cstdint>

namespace plexus::asio {

// A reserved delivery-guarantee axis. Today the only value is `unspecified`: TCP and
// AF_UNIX are both lossless ordered streams, so this axis does not distinguish a
// transport until a lossy datagram backend exists. The selector signature TAKES this
// parameter so the guarantee enumerators can land later WITHOUT reshaping select() or
// its callers.
enum class reliability_hint : std::uint8_t
{
    unspecified = 0,
};

// The tag the multiplexing transport maps to one of its member transports: a same-host
// peer is reached over the local stream, an off-host peer over the network stream.
enum class transport_kind : std::uint8_t
{
    local,
    remote,
};

// A small value object owned by the multiplexing transport. It holds NO transport
// handles and NO hint map — it is a pure function of the endpoint scheme (plus the
// reserved reliability axis). Same-host detection reads ep.scheme: "unix"/"inproc" are
// same-host (local); everything else, INCLUDING an unrecognized scheme, classifies
// remote — the most-restrictive-to-leak default, so a same-host-confined peer is never
// reached over an unknown transport.
//
// transport_kind::local names the same-host TIER, not a concrete transport. The current
// multiplexing transport maps that tier to its only same-host member, AF_UNIX — so an
// "inproc" endpoint (also same-host) classifies local but has no member to bind/dial it
// through this mux. An "inproc" endpoint must therefore not be dialed through this mux
// until it owns an in-process member; the classification is correct for the tier, the
// routing is bounded by the member set the mux composes.
class transport_selector
{
public:
    [[nodiscard]] transport_kind select(const io::endpoint &ep,
                                        reliability_hint /*reserved, ignored today*/) const noexcept
    {
        if(ep.scheme == "unix" || ep.scheme == "inproc")
            return transport_kind::local;
        return transport_kind::remote;
    }
};

}

#endif
