#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_UDP_INBOUND_DEMUX_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_UDP_INBOUND_DEMUX_H

#include <asio/ip/udp.hpp>
#include <asio/ip/address.hpp>

#include <cstddef>
#include <cstdint>
#include <utility>
#include <functional>
#include <unordered_map>

namespace plexus::asio {

class udp_channel;

namespace detail {

// Sender (addr,port) -> Channel* map: the whole game of a connectionless
// transport (no plexus analog — the stream transports own a per-connection socket
// the kernel demuxes; a datagram transport shares ONE bound socket and must demux
// completions in userspace). lookup(sender) returns the channel for an already-seen
// peer, or nullptr for a never-seen source — the MISS the transport turns into a
// synthesized accept. The transport owns the channels (unique_ptr); the demux holds
// non-owning raw refs keyed by endpoint and is the transport's PRIVATE state (the
// engine registry stays transport-agnostic). Templated on Channel so a non-udp
// datagram transport (e.g. DTLS) reuses the SAME flood-bound endpoint hash + cap
// without copying the security logic; udp_inbound_demux below binds it to
// udp_channel for udp_transport's existing call sites.
//
// THREAT (T-15-03): a UDP source address is forgeable, so a spoofed-source flood
// could mint unbounded "accept" channels. The transport synthesizes only a PENDING
// channel per new source and the handshake ARQ — not the source endpoint — is what
// authenticates identity; the demux bounds the live peer count so the flood cannot
// exhaust memory. insert() past the cap is refused (returns false) so the caller
// drops the datagram rather than growing without bound.
template<typename Channel>
class basic_inbound_demux
{
public:
    using endpoint_type = ::asio::ip::udp::endpoint;
    static constexpr std::size_t default_max_peers = 4096;

    explicit basic_inbound_demux(std::size_t max_peers = default_max_peers) noexcept
        : m_max_peers(max_peers)
    {
    }

    [[nodiscard]] Channel *lookup(const endpoint_type &sender) const
    {
        auto it = m_peers.find(sender);
        return it == m_peers.end() ? nullptr : it->second;
    }

    // Returns false when the peer cap is already reached (the flood guard) so the
    // caller drops the new source rather than admitting it.
    bool insert(const endpoint_type &sender, Channel *channel)
    {
        if(m_peers.size() >= m_max_peers)
            return false;
        m_peers.emplace(sender, channel);
        return true;
    }

    void erase(const endpoint_type &sender) { m_peers.erase(sender); }

    [[nodiscard]] std::size_t size() const noexcept { return m_peers.size(); }

private:
    // Hash the endpoint DIRECTLY off its packed (address-bytes, port) — NO per-datagram
    // textual key is minted, so the hot inbound lookup (once per received datagram) is
    // allocation-free, honoring the "no allocation on the steady-state hot path" invariant.
    // v4 packs into the low 4 bytes; v6 mixes all 16. asio's endpoint operator== resolves
    // collisions exactly (the map's key equality), so the hash need only spread well.
    struct endpoint_hash
    {
        std::size_t operator()(const endpoint_type &ep) const noexcept
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

    std::size_t m_max_peers;
    std::unordered_map<endpoint_type, Channel *, endpoint_hash> m_peers;
};

// The plain-UDP binding: udp_transport's existing call sites stay untouched.
using udp_inbound_demux = basic_inbound_demux<udp_channel>;

}

}

#endif
