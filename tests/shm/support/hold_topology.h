#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_HOLD_TOPOLOGY_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_HOLD_TOPOLOGY_H

#include "support/hold_probe.h"
#include "support/hold_tally.h"
#include "support/co_host_node.h"

#include "plexus/asio/stream_socket_options.h"

#include "plexus/io/reliability.h"

#include "plexus/publisher.h"
#include "plexus/topic_qos.h"
#include "plexus/subscriber.h"

#include <span>
#include <chrono>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>

namespace plexus::testing {

// The cap puts the publisher's ring on its 256-cell tier of 4 KiB slots, which every payload of a run fits.
inline plexus::topic_qos reliable_hinted_topic()
{
    plexus::topic_qos qos = hinted_topic();
    qos.reliability       = plexus::io::reliability::reliable;
    qos.max_message_bytes = 4096;
    return qos;
}

// Small kernel buffers on the publisher's and the newcomer's sockets let a fill the ring holds back the newcomer's wire.
inline plexus::asio::stream_socket_options small_socket_buffers()
{
    plexus::asio::stream_socket_options sockets;
    sockets.so_sndbuf = 64 * 1024;
    sockets.so_rcvbuf = 64 * 1024;
    return sockets;
}

// A hinted publisher of a reliable topic, a reader engaged on its ring and a newcomer, each on its own io_context.
struct hold_topology
{
    hold_topology(std::uint32_t total, std::uint32_t measured_from)
            : region(co_host_region("swh"))
            , fqn("switch_hold.topic")
            , publisher("publisher", 0x61, region, small_socket_buffers())
            , reader("reader", 0x62, region)
            , newcomer("newcomer", 0x63, region, small_socket_buffers())
            , reader_tally(total, measured_from)
            , newcomer_tally(total, measured_from)
            , drops(fqn)
            , pub(publisher.node, fqn, reliable_hinted_topic())
            , reader_sub(reader.node, fqn, hinted_subscriber(), [this](std::span<const std::byte> bytes) { reader_tally.on_delivery(bytes); })
            , newcomer_sub(newcomer.node, fqn, hinted_subscriber(), [this](std::span<const std::byte> bytes) { newcomer_tally.on_delivery(bytes); })
            , payload()
    {
        publisher.node.router().add_observer(drops);
        publisher.node.listen({"tcp", "127.0.0.1:0"});
    }

    void publish(std::uint32_t index)
    {
        write_hold_payload(payload, index, hold_now_ns());
        pub.publish(payload);
    }

    void dial(co_host_node &subscriber)
    {
        subscriber.node.dial({"tcp", "127.0.0.1:" + std::to_string(publisher.remote.port())});
    }

    // What the publisher's wire channel to the newcomer still holds; read on the publisher's executor.
    std::size_t newcomer_backlog()
    {
        std::size_t backlog = 0;
        publisher.node.router().registry().for_each_connected(
                [&](const plexus::node_id &, auto &session)
                {
                    if(session.peer_identity() == newcomer.node.id())
                        backlog = session.msg_peer().channel.backpressured();
                });
        return backlog;
    }

    std::string region;
    std::string fqn;
    co_host_node publisher;
    co_host_node reader;
    co_host_node newcomer;
    hold_tally reader_tally;
    hold_tally newcomer_tally;
    topic_drops drops;
    plexus::publisher<> pub;
    plexus::subscriber<> reader_sub;
    plexus::subscriber<> newcomer_sub;
    std::vector<std::byte> payload;
};

template<typename Done>
bool step_until(co_host_node &a, co_host_node &b, Done done, std::chrono::milliseconds budget)
{
    const auto deadline = std::chrono::steady_clock::now() + budget;
    while(!done())
    {
        if(std::chrono::steady_clock::now() >= deadline)
            return false;
        a.run_for(std::chrono::milliseconds{1});
        b.run_for(std::chrono::milliseconds{1});
    }
    return true;
}

// The reader is switched once a warm-up message reaches the publisher's ring, which only a switched reader routes to.
inline bool engage_reader(hold_topology &t, ring_probe &probe)
{
    t.dial(t.reader);
    if(!step_until(t.publisher, t.reader, [&] { return t.reader.reads_ring_of(t.publisher, t.fqn); }, std::chrono::seconds{5}) || !probe.attach(t.region, t.fqn, t.publisher.node.id()))
        return false;
    const std::uint64_t before = probe.tail();
    for(std::uint32_t warm = 0; probe.tail() == before && warm < 500; ++warm)
    {
        t.publish(k_hold_warm_bit | warm);
        step_until(t.publisher, t.reader, [] { return false; }, std::chrono::milliseconds{4});
    }
    return probe.tail() != before;
}

// The newcomer's ready leaves, and the publisher has not read it.
inline bool attach_newcomer(hold_topology &t)
{
    t.dial(t.newcomer);
    if(!step_until(t.publisher, t.newcomer, [&] { return t.newcomer.reads_ring_of(t.publisher, t.fqn); }, std::chrono::seconds{5}))
        return false;
    t.newcomer.run_for(std::chrono::milliseconds{20});
    return true;
}

}

#endif
