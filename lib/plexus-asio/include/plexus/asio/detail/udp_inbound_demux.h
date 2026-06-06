#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_UDP_INBOUND_DEMUX_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_UDP_INBOUND_DEMUX_H

#include <asio/ip/udp.hpp>

#include <string>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <unordered_map>

namespace plexus::asio {

class udp_channel;

namespace detail {

// Sender (addr,port) -> udp_channel* map: the whole game of a connectionless
// transport (no plexus analog — the stream transports own a per-connection socket
// the kernel demuxes; UDP shares ONE bound socket and must demux completions in
// userspace). lookup(sender) returns the channel for an already-seen peer, or
// nullptr for a never-seen source — the MISS the transport turns into a synthesized
// accept. The transport owns the channels (unique_ptr); the demux holds non-owning
// raw refs keyed by endpoint and is the transport's PRIVATE state (the engine
// registry stays transport-agnostic).
//
// THREAT (T-15-03): a UDP source address is forgeable, so a spoofed-source flood
// could mint unbounded "accept" channels. The transport synthesizes only a PENDING
// channel per new source and the handshake ARQ — not the source endpoint — is what
// authenticates identity; the demux bounds the live peer count so the flood cannot
// exhaust memory. insert() past the cap is refused (returns false) so the caller
// drops the datagram rather than growing without bound.
class udp_inbound_demux
{
public:
    using endpoint_type = ::asio::ip::udp::endpoint;
    static constexpr std::size_t default_max_peers = 4096;

    explicit udp_inbound_demux(std::size_t max_peers = default_max_peers) noexcept
        : m_max_peers(max_peers)
    {
    }

    [[nodiscard]] udp_channel *lookup(const endpoint_type &sender) const
    {
        auto it = m_peers.find(key_of(sender));
        return it == m_peers.end() ? nullptr : it->second;
    }

    // Returns false when the peer cap is already reached (the flood guard) so the
    // caller drops the new source rather than admitting it.
    bool insert(const endpoint_type &sender, udp_channel *channel)
    {
        if(m_peers.size() >= m_max_peers)
            return false;
        m_peers.emplace(key_of(sender), channel);
        return true;
    }

    void erase(const endpoint_type &sender) { m_peers.erase(key_of(sender)); }

    [[nodiscard]] std::size_t size() const noexcept { return m_peers.size(); }

private:
    // A printable (addr,port) key — stable across the address family and cheap to
    // hash. The textual address is the same string the channel reports back as its
    // remote_endpoint, so the key and the endpoint identity stay in lockstep.
    static std::string key_of(const endpoint_type &ep)
    {
        return ep.address().to_string() + ":" + std::to_string(ep.port());
    }

    std::size_t m_max_peers;
    std::unordered_map<std::string, udp_channel *> m_peers;
};

}

}

#endif
