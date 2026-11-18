#ifndef HPP_GUARD_TESTS_INTEGRATION_FORWARD_PUBSUB_LINE_H
#define HPP_GUARD_TESTS_INTEGRATION_FORWARD_PUBSUB_LINE_H

#include "test_forward_pubsub_common.h"

namespace forward_pubsub_fixture {

struct pubsub_line
{
    inproc_bus<manual_clock> bus;
    inproc_executor<manual_clock> ex{bus};
    transport_t t_pub{ex, bus};
    transport_t t_relay{ex, bus};
    transport_t t_sub{ex, bus};
    plexus::log::null_logger sink;

    engine pub;
    relay_engine relay;
    engine sub;

    node_id id_pub{make_id(0xB0)};
    node_id id_relay{make_id(0x2A)};
    node_id id_sub{make_id(0x5B)};
    endpoint ep_pub{"inproc", "pubsub-pub"};
    endpoint ep_relay{"inproc", "pubsub-relay"};
    endpoint ep_sub{"inproc", "pubsub-sub"};

    std::vector<delivery> sub_got;
    std::vector<delivery> pub_got;

    explicit pubsub_line(plexus::io::forward_options fwd = {})
            : pub(t_pub, ex, make_cfg(0xB0), k_long_timeout, forever_cfg(), k_seed, sink)
            , relay(t_relay, ex, make_cfg(0x2A), k_long_timeout, forever_cfg(), k_seed, sink, false, plexus::io::global_default_max_message_bytes, {}, {}, {}, fwd)
            , sub(t_sub, ex, make_cfg(0x5B), k_long_timeout, forever_cfg(), k_seed, sink)
    {
        sub.on_message_route([this](std::string_view fqn, std::span<const std::byte> b, const plexus::io::message_info &info)
                             { sub_got.push_back({std::string{fqn}, to_string(b), info}); });
        pub.on_message_route([this](std::string_view fqn, std::span<const std::byte> b, const plexus::io::message_info &info)
                             { pub_got.push_back({std::string{fqn}, to_string(b), info}); });
        pub.listen(ep_pub);
        relay.listen(ep_relay);
        sub.listen(ep_sub);
    }

    void drive()
    {
        ex.drain();
    }

    // The publisher declares the topic; the relay dials the publisher, or the publisher dials the relay
    // (the accepted-origin arm, whose slot key is provisional). Either way the publisher's declaration
    // folds onto the relay's topic table.
    void connect(std::string_view fqn, bool emit_source_identity, bool publisher_dials)
    {
        pub.messages().declare(fqn, plexus::topic_qos{}, std::nullopt, emit_source_identity);
        if(publisher_dials)
        {
            pub.note_peer(id_relay, ep_relay);
            pub.reach(id_relay);
        }
        else
        {
            relay.note_peer(id_pub, ep_pub);
            relay.reach(id_pub);
        }
        drive();
    }

    // The subscriber dials the relay and subscribes the topic through it; the relay folds the demand and
    // propagates an upstream subscribe onto the publisher's session.
    void subscribe_through_relay(std::string_view fqn)
    {
        sub.note_peer(id_relay, ep_relay);
        sub.subscribe(id_relay, fqn);
        drive();
    }

    void publish(std::string_view fqn, std::string_view body)
    {
        pub.publish(fqn, as_bytes(std::string{body}));
        drive();
    }

    bool no_direct_pub_sub_session() const
    {
        return !sub.is_connected(id_pub) && !pub.is_connected(id_sub);
    }
};

}

#endif
