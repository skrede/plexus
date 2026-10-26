#include "plexus/node.h"
#include "plexus/topic_query.h"
#include "plexus/node_options.h"
#include "plexus/participant_query.h"

#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_transport.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/graph/topic_record.h"
#include "plexus/graph/participant_record.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/post.hpp>
#include <asio/io_context.hpp>
#include <asio/executor_work_guard.hpp>

#include <span>
#include <array>
#include <atomic>
#include <thread>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string_view>

namespace {

using asio_node = plexus::basic_node<plexus::asio::asio_policy, plexus::asio::asio_transport>;

plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

struct asio_rig
{
    asio_rig()
            : io()
            , transport(io)
            , disc({})
            , node(io, disc, make_id(0x01), transport, plexus::node_options{})
    {
    }

    ::asio::io_context io;
    plexus::asio::asio_transport transport;
    plexus::discovery::static_discovery disc;
    asio_node node;
};

template<typename Query>
bool rejected_in_handler(asio_rig &rig, std::string_view expected, Query query)
{
    std::atomic<bool> rejected{false};
    ::asio::post(rig.io,
                 [&]
                 {
                     try
                     {
                         query();
                     }
                     catch(const std::runtime_error &e)
                     {
                         rejected.store(expected == e.what(), std::memory_order_relaxed);
                     }
                 });
    rig.io.poll();
    return rejected.load(std::memory_order_relaxed);
}

}

TEST_CASE("graph_blocking_helper_rejected participants_blocking called inside a handler of the node's executor is rejected instead of deadlocking",
          "[graph]")
{
    asio_rig rig;
    std::array<plexus::graph::participant_record, 4> buffer{};
    const auto query = [&] { plexus::participants_blocking(rig.node, std::span<plexus::graph::participant_record>{buffer}); };
    REQUIRE(rejected_in_handler(rig, "plexus: participants_blocking called inside a handler of the node's own executor", query));
}

TEST_CASE("graph_blocking_helper_rejected topics_blocking called inside a handler of the node's executor is rejected instead of deadlocking",
          "[graph]")
{
    asio_rig rig;
    std::array<plexus::graph::topic_record, 4> buffer{};
    const auto query = [&] { plexus::topics_blocking(rig.node, std::span<plexus::graph::topic_record>{buffer}); };
    REQUIRE(rejected_in_handler(rig, "plexus: topics_blocking called inside a handler of the node's own executor", query));
}

TEST_CASE("graph_blocking_helper_rejected a foreign thread's call is answered while another thread drives the executor", "[graph]")
{
    asio_rig rig;
    auto guard = ::asio::make_work_guard(rig.io);
    std::thread driver([&] { rig.io.run(); });

    std::array<plexus::graph::participant_record, 4> participants{};
    std::array<plexus::graph::topic_record, 4> topics{};
    const auto seen  = plexus::participants_blocking(rig.node, std::span<plexus::graph::participant_record>{participants});
    const auto named = plexus::topics_blocking(rig.node, std::span<plexus::graph::topic_record>{topics});

    guard.reset();
    rig.io.stop();
    driver.join();
    REQUIRE(seen.count == 0);
    REQUIRE(named.count == 0);
}
