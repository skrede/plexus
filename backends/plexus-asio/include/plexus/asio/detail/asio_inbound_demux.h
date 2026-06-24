#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_ASIO_INBOUND_DEMUX_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_ASIO_INBOUND_DEMUX_H

#include "plexus/datagram/detail/inbound_demux.h"

#include <asio/ip/udp.hpp>
#include <asio/ip/address.hpp>

#include <cstddef>
#include <cstdint>
#include <functional>

namespace plexus::asio {

class udp_channel;

namespace detail {

// Hash the endpoint directly off its packed (address-bytes, port) so the per-datagram inbound
// lookup mints no textual key (no hot-path allocation); asio's endpoint operator== resolves
// collisions exactly, so the hash need only spread well.
struct asio_endpoint_hash
{
    std::size_t operator()(const ::asio::ip::udp::endpoint &ep) const noexcept
    {
        const auto addr = ep.address();
        std::size_t h   = std::hash<std::uint16_t>{}(ep.port());
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

template<typename Channel>
using basic_inbound_demux = plexus::datagram::detail::basic_inbound_demux<Channel, ::asio::ip::udp::endpoint, asio_endpoint_hash>;

using udp_inbound_demux = basic_inbound_demux<udp_channel>;

}

}

#endif
