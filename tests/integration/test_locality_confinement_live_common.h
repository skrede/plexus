#ifndef HPP_GUARD_TESTS_INTEGRATION_LOCALITY_CONFINEMENT_LIVE_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_LOCALITY_CONFINEMENT_LIVE_COMMON_H

#include "test_locality_confinement_common.h"

#ifdef PLEXUS_HAVE_ASIO_MUX

namespace locality_confinement_fixture {

namespace pasio = plexus::asio;
namespace pio   = plexus::io;

// A per-instance owner-only temp dir + a SHORT AF_UNIX socket path (well under sun_path).
struct temp_sock
{
    std::string dir;
    std::string path;

    temp_sock()
    {
        char tmpl[]      = "/tmp/pxl-XXXXXX";
        const char *made = ::mkdtemp(tmpl);
        dir              = made ? made : "";
        path             = dir + "/s";
    }

    ~temp_sock()
    {
        if(!path.empty())
            ::unlink(path.c_str());
        if(!dir.empty())
            ::rmdir(dir.c_str());
    }
};

// A live two-transport fixture: a forwarder over the unix policy fans toward BOTH a real
// AF_UNIX channel (local tier) AND a real TCP channel (remote tier). The forwarder's
// channel_type is the unix_channel; the TCP end is wrapped behind the same concrete
// surface only conceptually — instead we run TWO forwarders, one per concrete policy, and
// attach each real channel to a forwarder of its own policy. Simpler and faithful: the
// gate is per-subscriber-tier, so a single forwarder with both real channels is what we
// want — but the two channel types differ. We therefore drive the gate over EACH real
// transport's own forwarder and assert the cross-tier confinement by reach mask.
//
// Concretely: stand up a real AF_UNIX dialed link and a real TCP dialed link, then for a
// forwarder fanning over the AF_UNIX channel assert a remote-confined topic is dropped and
// a local-confined topic is delivered; symmetrically over the TCP channel.

template<typename Policy, typename Transport, typename Channel>
struct live_link
{
    ::asio::io_context io;
    Transport transport{io};

    plexus::log::null_logger sink;
    pio::message_forwarder<Policy> pub_messages{sink};
    pio::message_forwarder<Policy> sub_messages{sink};
    pio::procedure_forwarder<Policy> pub_procedures{io, std::chrono::hours(1), sink};
    pio::procedure_forwarder<Policy> sub_procedures{io, std::chrono::hours(1), sink};

    pio::peer_context<Policy> pub_ctx;
    pio::peer_context<Policy> sub_ctx;
    std::optional<pio::peer_session<Policy>> publisher;  // the dialer end
    std::optional<pio::peer_session<Policy>> subscriber; // the accepted end

    std::vector<std::string> received;

    void wire()
    {
        transport.on_accepted(
                [this](std::unique_ptr<Channel> ch)
                {
                    sub_ctx.channel   = std::move(ch);
                    sub_ctx.node_name = "publisher-node";
                    subscriber.emplace(sub_ctx, io, make_cfg(0x01), std::chrono::hours(1), sub_messages, sub_procedures, true, sink);
                    subscriber->on_message([this](std::string_view, std::span<const std::byte> d) { received.emplace_back(reinterpret_cast<const char *>(d.data()), d.size()); });
                    subscriber->start();
                });
        transport.on_dialed(
                [this](std::unique_ptr<Channel> ch, const pio::endpoint &)
                {
                    pub_ctx.channel   = std::move(ch);
                    pub_ctx.node_name = "subscriber-node";
                    publisher.emplace(pub_ctx, io, make_cfg(0x02), std::chrono::hours(1), pub_messages, pub_procedures, false, sink);
                    publisher->start();
                });
    }

    template<typename Pred>
    void pump_until(Pred pred)
    {
        auto bound = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while(!pred() && std::chrono::steady_clock::now() < bound)
            io.poll();
    }

    void settle(std::chrono::milliseconds window = std::chrono::milliseconds(20))
    {
        auto bound = std::chrono::steady_clock::now() + window;
        while(std::chrono::steady_clock::now() < bound)
            io.poll();
    }
};

// Drive one live link's confinement: with the subscriber attached for fan-out, a topic
// whose reach EXCLUDES the link's tier delivers nothing; a topic whose reach INCLUDES it
// delivers. `tier_mask` is the reach that should include this link (local for AF_UNIX,
// remote for TCP); `excluded_mask` is one that should not.
template<typename Link>
inline void prove_link_confinement(Link &l, const std::string &fqn, locality including_mask, locality excluding_mask)
{
    l.pump_until([&] { return l.publisher && l.subscriber && l.publisher->is_complete() && l.subscriber->is_complete(); });
    REQUIRE(l.publisher->is_complete());
    REQUIRE(l.subscriber->is_complete());

    REQUIRE(l.sub_messages.attach(l.subscriber->msg_peer(), fqn));
    REQUIRE(l.pub_messages.attach_for_fanout(l.publisher->msg_peer(), fqn));
    l.settle();

    const std::string payload = "live-confined-payload";

    // Excluded reach: the publisher's fan-out gate drops the send — nothing arrives.
    l.pub_messages.declare(fqn, plexus::topic_qos{.reach = excluding_mask});
    l.received.clear();
    l.pub_messages.publish(fqn, as_bytes(payload), l.publisher->session_id());
    l.settle(std::chrono::milliseconds(40));
    REQUIRE(l.received.empty()); // an off-tier topic NEVER crosses this transport

    // Including reach: the same fan-out delivers over the real transport.
    l.pub_messages.declare(fqn, plexus::topic_qos{.reach = including_mask});
    l.received.clear();
    l.pub_messages.publish(fqn, as_bytes(payload), l.publisher->session_id());
    l.pump_until([&] { return !l.received.empty(); });
    REQUIRE(l.received.size() == 1);
    REQUIRE(l.received.front() == payload);
}

}

#endif

#endif
