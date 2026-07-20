#include "plexus/topic_query.h"
#include "plexus/node.h"
#include "plexus/node_options.h"
#include "plexus/participant_query.h"

#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_transport.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/graph/topic_record.h"
#include "plexus/graph/participant_record.h"

#include "plexus/match/key_pattern.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <span>
#include <array>
#include <atomic>
#include <string>
#include <thread>
#include <cstddef>
#include <cstdint>

namespace {

namespace pasio = plexus::asio;

using asio_node = plexus::basic_node<pasio::asio_policy, pasio::asio_transport>;

plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

plexus::io::endpoint make_ep(int n)
{
    return plexus::io::endpoint{"tcp", "10.0.0." + std::to_string(n) + ":7000"};
}

plexus::node_options lazy_opts()
{
    plexus::node_options opts;
    opts.redial_seed = 0xC0FFEEu;
    return opts;
}

bool direct_only(const plexus::graph::participant_record &rec)
{
    return rec.origin.how == plexus::graph::observation::directly_observed && !rec.origin.reporter.has_value() && !rec.reach.via.has_value();
}

constexpr std::array<std::string_view, 4> k_topic_names{"sensor/imu", "sensor/pose", "drive/cmd", "drive/odom"};

plexus::graph::topic_edge edge_for(int slot)
{
    return plexus::graph::topic_edge{make_id(static_cast<std::uint8_t>(0x10 + slot)), k_topic_names[static_cast<std::size_t>(slot)], "sensor/Imu",
                                     std::optional<std::uint64_t>{0x1A2Bu}, plexus::graph::topic_role::publisher};
}

}

// A foreign thread reads the snapshot via participants_blocking in a loop while the OWNING
// executor thread churns peers (note_peer/forget). The helper posts the fill onto the executor, so the
// awareness table is only ever touched on that one thread — race-free by construction, and under
// -fsanitize=thread the run reports no data race on the live table.
TEST_CASE("foreign_thread: participants_blocking reads a churning table race-free", "[graph]")
{
    ::asio::io_context      io;
    pasio::asio_transport   transport{io};
    plexus::discovery::static_discovery disc{{}};
    asio_node               node{io, disc, make_id(0x01), transport, lazy_opts()};

    constexpr int k_peers              = 6;
    constexpr int k_foreign_iterations = 2000;

    std::atomic<bool> foreign_done{false};

    // The owning thread: it alone touches the table (churn) and it alone runs the posted fills.
    std::thread executor(
            [&]
            {
                int tick = 0;
                while(!foreign_done.load(std::memory_order_acquire))
                {
                    const int slot = tick % k_peers;
                    if(tick % 2 == 0)
                        node.router().note_peer(make_id(static_cast<std::uint8_t>(0x10 + slot)), make_ep(slot));
                    else
                        node.router().forget(make_id(static_cast<std::uint8_t>(0x10 + slot)));
                    if(io.stopped())
                        io.restart();
                    io.poll();
                    ++tick;
                }
                if(io.stopped())
                    io.restart();
                io.poll();
            });

    std::thread foreign(
            [&]
            {
                for(int i = 0; i < k_foreign_iterations; ++i)
                {
                    std::array<plexus::graph::participant_record, k_peers> buffer{};
                    const auto result = participants_blocking(node, std::span<plexus::graph::participant_record>{buffer});
                    REQUIRE(result.count <= buffer.size());
                    for(std::size_t j = 0; j < result.count; ++j)
                        REQUIRE(direct_only(buffer[j]));
                }
                foreign_done.store(true, std::memory_order_release);
            });

    foreign.join();
    executor.join();
}

// The topic twin of the participant crossing: a foreign thread reads the snapshot in a loop while
// the OWNING executor thread folds edges into the table. topics_blocking posts the fill onto the
// executor, so the topic table is only ever touched on that one thread — race-free by
// construction, and under -fsanitize=thread the run reports no data race on the live table.
TEST_CASE("foreign_thread: topics_blocking reads a churning topic table race-free", "[graph]")
{
    ::asio::io_context                  io;
    pasio::asio_transport               transport{io};
    plexus::discovery::static_discovery disc{{}};
    asio_node                           node{io, disc, make_id(0x01), transport, lazy_opts()};

    constexpr int k_foreign_iterations = 2000;
    const auto    sensors              = plexus::match::key_pattern::make("sensor/*");
    REQUIRE(sensors.has_value());

    std::atomic<bool> foreign_done{false};

    // The owning thread: it alone folds edges into the table and it alone runs the posted fills.
    std::thread executor(
            [&]
            {
                int tick = 0;
                while(!foreign_done.load(std::memory_order_acquire))
                {
                    node.message_forwarder().note_topic_edge(edge_for(tick % static_cast<int>(k_topic_names.size())));
                    if(io.stopped())
                        io.restart();
                    io.poll();
                    ++tick;
                }
                if(io.stopped())
                    io.restart();
                io.poll();
            });

    std::thread foreign(
            [&]
            {
                for(int i = 0; i < k_foreign_iterations; ++i)
                {
                    std::array<plexus::graph::topic_record, k_topic_names.size()> buffer{};
                    const std::span<plexus::graph::topic_record> out{buffer};
                    REQUIRE(topics_blocking(node, out).count <= buffer.size());
                    // The filter crosses with the fill and is applied on the owning executor, never
                    // by re-reading the table here. Only what one snapshot returned can be asserted:
                    // the table grows between two calls, so no relation ACROSS snapshots holds.
                    const auto sensed = topics_blocking(node, out, *sensors);
                    for(std::size_t j = 0; j < sensed.count; ++j)
                        REQUIRE(buffer[j].name.starts_with("sensor/"));
                }
                foreign_done.store(true, std::memory_order_release);
            });

    foreign.join();
    executor.join();
}
