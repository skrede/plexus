#ifndef HPP_GUARD_TESTS_INTEGRATION_RECORDING_OBSERVER_H
#define HPP_GUARD_TESTS_INTEGRATION_RECORDING_OBSERVER_H

#include "plexus/io/observer.h"
#include "plexus/io/peer_kind.h"
#include "plexus/io/message_info.h"
#include "plexus/io/handshake_fsm.h"
#include "plexus/io/observation_events.h"

#include "plexus/wire/topic_hash.h"

#include "plexus/node_id.h"

#include <map>
#include <memory>
#include <string>
#include <cstdint>
#include <string_view>

// The reusable recording observer the lifecycle/readiness suites assert against.
// It subclasses the public observer and counts each EDGE per peer — it never
// reaches into the session for an internal emit counter (the world-class bar
// rejects a test-only mutator). The keys are the peer's stable node_id, so a test
// inspecting one peer's edges is unaffected by another's. The last rejected reason
// and the last observed peer_kind are recorded per peer for the reason/kind
// assertions. Every method body stays tiny (the per-peer struct does the work),
// keeping the consuming test files within the file-size guidance.
class recording_observer final : public plexus::io::observer
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

    // The per-topic data-path edge tally, keyed by the delivered fqn (mirroring the
    // per-peer struct so the tap bodies stay tiny). published counts the once-per-publish
    // edge, delivered the once-per-destination edge, the rpc fields the request/reply
    // edges. last_view_owner / last_view_use_count capture the delivered view's owner so a
    // test can assert the view BORROWS the buffer (a shared addref) rather than copies it.
    struct topic_counts
    {
        int published{0};
        int delivered{0};
        int rpc_call{0};
        int rpc_serve{0};
        int rpc_reply{0};
        int publisher_declared{0};
        int publisher_dropped{0};
        int subscriber_registered{0};
        int subscriber_retired{0};
        int unsubscribed{0};
        std::shared_ptr<const void> last_view_owner{};
        long last_view_use_count{0};
    };

    struct participant_counts
    {
        int created{0};
        int destroyed{0};
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

    void on_message_published(std::string_view fqn, const plexus::io::message_view &) override
    {
        ++m_topics[std::string{fqn}].published;
    }

    void on_message_delivered(std::string_view fqn, const plexus::io::message_info &, const plexus::io::message_view &view) override
    {
        auto &t = m_topics[std::string{fqn}];
        ++t.delivered;
        t.last_view_owner     = view.owner();
        t.last_view_use_count = view.owner().use_count();
    }

    void on_rpc_call(std::string_view fqn, const plexus::io::rpc_view &) override
    {
        ++m_topics[std::string{fqn}].rpc_call;
    }

    void on_rpc_serve(std::string_view fqn, const plexus::io::rpc_view &) override
    {
        ++m_topics[std::string{fqn}].rpc_serve;
    }

    void on_rpc_reply(std::string_view fqn, const plexus::io::rpc_reply_view &) override
    {
        ++m_topics[std::string{fqn}].rpc_reply;
    }

    void on_participant(const plexus::io::participant_event &ev) override
    {
        auto &c = m_participants[ev.self];
        if(ev.edge == plexus::io::participant_edge::created)
            ++c.created;
        else
            ++c.destroyed;
    }

    void on_endpoint(std::string_view fqn, const plexus::io::endpoint_event &ev) override
    {
        auto &t = m_topics[std::string{fqn}];
        switch(ev.edge)
        {
            case plexus::io::endpoint_edge::publisher_declared:
                ++t.publisher_declared;
                break;
            case plexus::io::endpoint_edge::publisher_dropped:
                ++t.publisher_dropped;
                break;
            case plexus::io::endpoint_edge::subscriber_registered:
                ++t.subscriber_registered;
                break;
            case plexus::io::endpoint_edge::subscriber_retired:
                ++t.subscriber_retired;
                break;
        }
    }

    void on_qos_change(const plexus::io::qos_change_event &ev) override
    {
        if(ev.edge == plexus::io::qos_edge::unsubscribed)
            ++m_unsubscribed_by_hash[ev.topic_hash];
    }

    // Opt into the data-path taps: this observer counts the message/rpc edges, so the
    // engine must fan them here (a lifecycle-only observer leaves the default false and
    // pays nothing on the hot path).
    bool observes_data_path() const override
    {
        return true;
    }

    // Per-peer accessor: an absent peer reads as all-zero (the default-constructed
    // counts), so a test can assert "this edge never fired" without a contains check.
    const counts &for_peer(const plexus::node_id &id) const
    {
        auto it = m_peers.find(id);
        return it == m_peers.end() ? m_empty : it->second;
    }

    // Per-topic accessor: an unseen fqn reads as all-zero, so a test can assert a tap
    // never fired (the counter is still zero before the executor pumps) without a
    // contains check.
    const topic_counts &for_topic(std::string_view fqn) const
    {
        auto it = m_topics.find(std::string{fqn});
        return it == m_topics.end() ? m_empty_topic : it->second;
    }

    // The single accepted peer is keyed by a synthetic inbound identity the test does
    // not pre-compute; expose the sole recorded peer for the accepting-node suites.
    const counts &only_peer() const
    {
        return m_peers.empty() ? m_empty : m_peers.begin()->second;
    }

    // Per-participant accessor: an unseen node_id reads as all-zero, mirroring for_peer.
    const participant_counts &for_participant(const plexus::node_id &id) const
    {
        auto it = m_participants.find(id);
        return it == m_participants.end() ? m_empty_participant : it->second;
    }

    // The unsubscribe edge rides on_qos_change keyed by topic_hash alone (the POD carries
    // no fqn), so the accessor rehashes the fqn to read its tally.
    int unsubscribed_for(std::string_view fqn) const
    {
        auto it = m_unsubscribed_by_hash.find(plexus::wire::fqn_topic_hash(fqn));
        return it == m_unsubscribed_by_hash.end() ? 0 : it->second;
    }

    std::size_t recorded_peers() const
    {
        return m_peers.size();
    }

private:
    std::map<plexus::node_id, counts> m_peers;
    std::map<plexus::node_id, participant_counts> m_participants;
    std::map<std::string, topic_counts> m_topics;
    std::map<std::uint64_t, int> m_unsubscribed_by_hash;
    counts m_empty{};
    participant_counts m_empty_participant{};
    topic_counts m_empty_topic{};
};

#endif
