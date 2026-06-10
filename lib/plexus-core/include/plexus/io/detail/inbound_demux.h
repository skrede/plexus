#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_INBOUND_DEMUX_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_INBOUND_DEMUX_H

#include <cstddef>
#include <utility>
#include <unordered_map>

namespace plexus::io::detail {

// Sender (addr,port) -> Channel* map: the whole game of a connectionless
// transport (no plexus analog — the stream transports own a per-connection socket
// the kernel demuxes; a datagram transport shares ONE bound socket and must demux
// completions in userspace). lookup(sender) returns the channel for an already-seen
// peer, or nullptr for a never-seen source — the MISS the transport turns into a
// synthesized accept. The transport owns the channels (unique_ptr); the demux holds
// non-owning raw refs keyed by endpoint and is the transport's PRIVATE state (the
// engine registry stays transport-agnostic). Generic over Channel, Endpoint, and Hash
// so any datagram transport (plain UDP, DTLS, a future backend) reuses the SAME
// flood-bound endpoint map + cap without copying the security logic; a backend binds
// the endpoint type and its hash and re-exposes a one-argument alias for its call sites.
//
// THREAT (spoofed-source flood): a UDP source address is forgeable, so a spoofed-source
// flood could mint unbounded "accept" channels. The transport synthesizes only a PENDING
// channel per new source and the handshake ARQ — not the source endpoint — is what
// authenticates identity; the demux bounds the live peer count so the flood cannot
// exhaust memory. insert() past the cap is refused (returns false) so the caller
// drops the datagram rather than growing without bound.
template<typename Channel, typename Endpoint, typename Hash>
class basic_inbound_demux
{
public:
    using endpoint_type = Endpoint;
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
    // caller drops the new source rather than admitting it. insert_or_assign (NOT
    // emplace): a re-dial / re-accept to a sender that already has an entry OVERWRITES
    // the stale pointer with the new channel — emplace was a no-op on an existing key,
    // so a re-dial silently kept the OLD (possibly torn-down) pointer and the new
    // channel's inbound never routed. An overwrite to an already-present key does not
    // grow the map, so the cap guard need only gate genuinely new keys.
    bool insert(const endpoint_type &sender, Channel *channel)
    {
        if(m_peers.size() >= m_max_peers && m_peers.find(sender) == m_peers.end())
            return false;
        m_peers.insert_or_assign(sender, channel);
        return true;
    }

    void erase(const endpoint_type &sender) { m_peers.erase(sender); }

    [[nodiscard]] std::size_t size() const noexcept { return m_peers.size(); }

private:
    std::size_t m_max_peers;
    std::unordered_map<endpoint_type, Channel *, Hash> m_peers;
};

}

#endif
