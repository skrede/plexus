#ifndef HPP_GUARD_PLEXUS_DATAGRAM_DETAIL_INBOUND_DEMUX_H
#define HPP_GUARD_PLEXUS_DATAGRAM_DETAIL_INBOUND_DEMUX_H

#include <cstddef>
#include <utility>
#include <unordered_map>

namespace plexus::datagram::detail {

// A sender (addr,port) -> Channel map for a connectionless transport that shares one bound socket
// and must demux completions in userspace. lookup(sender) returns the channel for an already-seen
// peer, or nullptr for a never-seen source (the miss the transport turns into a synthesized
// accept); the demux holds non-owning refs to channels the transport owns.
//
// THREAT (spoofed-source flood): a UDP source address is forgeable, so the demux bounds the live
// peer count and refuses insert() past the cap so a flood cannot exhaust memory; the handshake ARQ,
// not the source endpoint, is what authenticates identity.
template<typename Channel, typename Endpoint, typename Hash>
class basic_inbound_demux
{
public:
    using endpoint_type                            = Endpoint;
    static constexpr std::size_t default_max_peers = 4096;

    explicit basic_inbound_demux(std::size_t max_peers = default_max_peers) noexcept
            : m_max_peers(max_peers)
    {
    }

    Channel *lookup(const endpoint_type &sender) const
    {
        auto it = m_peers.find(sender);
        return it == m_peers.end() ? nullptr : it->second;
    }

    // Returns false when the peer cap is already reached (the flood guard). insert_or_assign so a
    // re-dial/re-accept to an existing sender overwrites the stale (possibly torn-down) channel;
    // an overwrite does not grow the map, so the cap guard need only gate genuinely new keys.
    bool insert(const endpoint_type &sender, Channel *channel)
    {
        if(m_peers.size() >= m_max_peers && m_peers.find(sender) == m_peers.end())
            return false;
        m_peers.insert_or_assign(sender, channel);
        return true;
    }

    void erase(const endpoint_type &sender)
    {
        m_peers.erase(sender);
    }

    // Erase only if the entry still maps to `channel`: a re-dial may have already overwritten it
    // with a new channel before the old channel's deferred destruction runs, and an unconditional
    // erase would drop the live re-dial's entry.
    void erase_if_matches(const endpoint_type &sender, const Channel *channel)
    {
        auto it = m_peers.find(sender);
        if(it != m_peers.end() && it->second == channel)
            m_peers.erase(it);
    }

    std::size_t size() const noexcept
    {
        return m_peers.size();
    }

private:
    std::size_t m_max_peers;
    std::unordered_map<endpoint_type, Channel *, Hash> m_peers;
};

}

#endif
