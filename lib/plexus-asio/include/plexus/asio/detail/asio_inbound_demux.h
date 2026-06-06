#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_ASIO_INBOUND_DEMUX_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_ASIO_INBOUND_DEMUX_H

#include "plexus/io/detail/inbound_demux.h"

#include <asio/ip/udp.hpp>
#include <asio/ip/address.hpp>

#include <cstddef>
#include <cstdint>
#include <functional>

namespace plexus::asio {

class udp_channel;

namespace detail {

// The asio endpoint+hash binding for the generic core demux: the irreducible backend
// seam. Hash the endpoint DIRECTLY off its packed (address-bytes, port) — NO per-datagram
// textual key is minted, so the hot inbound lookup (once per received datagram) is
// allocation-free, honoring the "no allocation on the steady-state hot path" invariant.
// v4 packs into the low 4 bytes; v6 mixes all 16. asio's endpoint operator== resolves
// collisions exactly (the map's key equality), so the hash need only spread well.
struct asio_endpoint_hash
{
    std::size_t operator()(const ::asio::ip::udp::endpoint &ep) const noexcept
    {
        const auto addr = ep.address();
        std::size_t h = std::hash<std::uint16_t>{}(ep.port());
        if(addr.is_v4())
        {
            h ^= std::hash<std::uint32_t>{}(addr.to_v4().to_uint()) + 0x9e3779b97f4a7c15ull + (h << 6) + (h >> 2);
            return h;
        }
        const auto bytes = addr.to_v6().to_bytes();
        for(auto b : bytes)
            h ^= std::hash<std::uint8_t>{}(b) + 0x9e3779b97f4a7c15ull + (h << 6) + (h >> 2);
        return h;
    }
};

// Bind the generic core demux to the asio udp endpoint + its hash. Every existing call
// site spells either this one-argument alias or udp_inbound_demux, so the consumer source
// is unchanged — only the defining file moved + split.
template<typename Channel>
using basic_inbound_demux =
    plexus::io::detail::basic_inbound_demux<Channel, ::asio::ip::udp::endpoint, asio_endpoint_hash>;

// The plain-UDP binding: udp_transport's existing call sites stay untouched.
using udp_inbound_demux = basic_inbound_demux<udp_channel>;

}

}

#endif
