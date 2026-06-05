#ifndef HPP_GUARD_TESTS_INTEGRATION_RECORDING_OBSERVER_H
#define HPP_GUARD_TESTS_INTEGRATION_RECORDING_OBSERVER_H

#include "plexus/io/peer_observer.h"
#include "plexus/io/peer_kind.h"
#include "plexus/io/handshake_fsm.h"

#include "plexus/node_id.h"

#include <map>
#include <string>
#include <cstdint>
#include <string_view>

// The reusable recording observer the lifecycle/readiness suites assert against.
// It subclasses the public peer_observer and counts each EDGE per peer — it never
// reaches into the session for an internal emit counter (the world-class bar
// rejects a test-only mutator). The keys are the peer's stable node_id, so a test
// inspecting one peer's edges is unaffected by another's. The last rejected reason
// and the last observed peer_kind are recorded per peer for the reason/kind
// assertions. Every method body stays tiny (the per-peer struct does the work),
// keeping the consuming test files within the file-size guidance.
class recording_observer final : public plexus::io::peer_observer
{
public:
    struct counts
    {
        int connected{0};
        int disconnected{0};
        int reconnected{0};
        int dead{0};
        int ready{0};
        int rejected{0};
        plexus::io::peer_kind last_kind{plexus::io::peer_kind::dialed};
        plexus::io::handshake_outcome last_reason{plexus::io::handshake_outcome::none};
    };

    void on_peer_connected(const plexus::node_id &id, std::string_view, plexus::io::peer_kind k) override
    {
        auto &c = m_peers[id];
        ++c.connected;
        c.last_kind = k;
    }

    void on_peer_disconnected(const plexus::node_id &id, std::string_view, plexus::io::peer_kind k) override
    {
        auto &c = m_peers[id];
        ++c.disconnected;
        c.last_kind = k;
    }

    void on_peer_reconnected(const plexus::node_id &id, std::string_view, plexus::io::peer_kind k) override
    {
        auto &c = m_peers[id];
        ++c.reconnected;
        c.last_kind = k;
    }

    void on_peer_dead(const plexus::node_id &id, std::string_view, plexus::io::peer_kind k) override
    {
        auto &c = m_peers[id];
        ++c.dead;
        c.last_kind = k;
    }

    void on_peer_ready(const plexus::node_id &id, std::string_view, plexus::io::peer_kind k) override
    {
        auto &c = m_peers[id];
        ++c.ready;
        c.last_kind = k;
    }

    void on_peer_rejected(const plexus::node_id &id, std::string_view, plexus::io::handshake_outcome reason) override
    {
        auto &c = m_peers[id];
        ++c.rejected;
        c.last_reason = reason;
    }

    // Per-peer accessor: an absent peer reads as all-zero (the default-constructed
    // counts), so a test can assert "this edge never fired" without a contains check.
    const counts &for_peer(const plexus::node_id &id) const
    {
        auto it = m_peers.find(id);
        return it == m_peers.end() ? m_empty : it->second;
    }

    // The single accepted peer is keyed by a synthetic inbound identity the test does
    // not pre-compute; expose the sole recorded peer for the accepting-node suites.
    const counts &only_peer() const
    {
        return m_peers.empty() ? m_empty : m_peers.begin()->second;
    }

    std::size_t recorded_peers() const { return m_peers.size(); }

private:
    std::map<plexus::node_id, counts> m_peers;
    counts m_empty{};
};

#endif
