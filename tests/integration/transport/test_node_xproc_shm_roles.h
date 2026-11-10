#ifndef HPP_GUARD_PLEXUS_TESTS_INTEGRATION_TEST_NODE_XPROC_SHM_ROLES_H
#define HPP_GUARD_PLEXUS_TESTS_INTEGRATION_TEST_NODE_XPROC_SHM_ROLES_H

#include "test_node_xproc_shm_ring.h"
#include "test_node_xproc_shm_common.h"

#include "plexus/node.h"
#include "plexus/publisher.h"
#include "plexus/subscriber.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/testing/pump.h"
#include "plexus/testing/platform.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <span>
#include <chrono>
#include <string>
#include <cstddef>
#include <cstdint>

#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>

namespace node_xproc_shm_fixture {

struct run_endpoints
{
    std::uint16_t sub_port;
    std::uint16_t pub_port;
    plexus::node_id sub_id;
    plexus::node_id pub_id;
    std::string sub_name;
    std::string pub_name;
};

struct run_setup
{
    const std::string &fqn;
    const std::string &small;
    const std::string &large;
    bool same_host;
    const run_endpoints &ends;
    coord &c;
};

inline run_endpoints make_endpoints()
{
    const std::uint16_t sub_port = static_cast<std::uint16_t>(41000 + 2 * (plexus::testing::process_id() % 9000));
    const std::uint16_t pub_port = sub_port + 1;
    return {sub_port, pub_port, make_id(0x51), make_id(0x52), "node.sub." + std::to_string(plexus::testing::process_id()), "node.pub." + std::to_string(plexus::testing::process_id())};
}

inline void tag_arrival(int fd, const run_setup &run, std::span<const std::byte> b)
{
    const std::string got(reinterpret_cast<const char *>(b.data()), b.size());
    const char tag  = got == run.small ? 'S' : (got == run.large ? 'L' : '?');
    const ssize_t w = ::write(fd, &tag, 1);
    (void)w;
}

inline void observe_ring(::asio::io_context &io, const run_setup &run, int fd)
{
    plexus::testing::pump_until(io, [&] { return run.c.ring_ready.load(std::memory_order_acquire) != 0; }, std::chrono::seconds(10));
    const bool seen   = ring_carries(run.fqn, as_bytes(run.small), std::chrono::seconds(10), [&io] { io.poll(); });
    const char marker = 'E';
    run.c.shm_seen.store(seen && ::write(fd, &marker, 1) == 1 ? 1u : 0u, std::memory_order_release);
}

inline void drain_wire(::asio::io_context &io)
{
    const auto until = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while(std::chrono::steady_clock::now() < until)
        io.poll();
}

inline void subscribe(const run_setup &run, int fd)
{
    ::asio::io_context io;
    plexus::discovery::static_discovery disc{{peer_card(run.ends.pub_name, run.ends.pub_id, run.ends.pub_port)}};
    pasio::same_host_transports ts{io};
    auto node = ts.make_node(disc, run.ends.sub_id, run.same_host ? dial_opts(true) : distinct_fingerprint_opts(true, 0xA1A1A1A1u));
    // Shared memory engages only when both ends' policies allow it, so the subscriber opts in with its own hint.
    plexus::io::subscriber_qos sub_qos;
    sub_qos.dispatch = plexus::io::dispatch_hint::frequent;
    plexus::subscriber<> sub{node, run.fqn, sub_qos, [&](std::span<const std::byte> b) { tag_arrival(fd, run, b); }};
    node.listen({"tcp", "127.0.0.1:" + std::to_string(run.ends.sub_port)});
    plexus::testing::pump_until(io, [&] { return node.router().is_connected(run.ends.pub_id); }, std::chrono::seconds(10));
    if(run.same_host)
        observe_ring(io, run, fd);
    drain_wire(io);
}

inline void run_subscriber(const run_setup &run, int read_fd, int write_fd)
{
    ::close(read_fd);
    subscribe(run, write_fd);
    run.c.consumer_done.store(1u, std::memory_order_release);
    ::close(write_fd);
    ::_exit(0);
}

inline void observe_region(const run_setup &run, medium_result &result, bool &ring_signaled)
{
    if(result.region_observed || !region_maps(run.fqn))
        return;
    result.region_observed = true;
    if(!ring_signaled)
        run.c.ring_ready.store(1u, std::memory_order_release);
    ring_signaled = true;
}

inline void publish_paced(::asio::io_context &io, same_host_node &node, plexus::publisher<> &pub, const run_setup &run, int fd, medium_result &result, bool &engaged)
{
    std::uint32_t over_cap_in_flight = 0;
    bool consumer_done               = false;
    bool ring_signaled               = false;
    const auto deadline              = std::chrono::steady_clock::now() + std::chrono::seconds(20);
    while(!consumer_done && std::chrono::steady_clock::now() < deadline)
    {
        io.poll();
        observe_region(run, result, ring_signaled);
        // Unpaced, the over-cap messages back the wire up for good once the subscriber falls behind: every fitting
        // message then queues behind them and the subscriber's poll never returns, its socket always readable. So a
        // pair goes out only once the subscribe is admitted and the last over-cap message has arrived.
        over_cap_in_flight -= take_tags(fd, result, engaged);
        if(node.count_subscribers(run.fqn) != 0 && over_cap_in_flight == 0)
        {
            pub.publish(as_bytes(run.small));
            pub.publish(as_bytes(run.large));
            ++over_cap_in_flight;
        }
        consumer_done = run.c.consumer_done.load(std::memory_order_acquire) != 0;
    }
}

inline void publish(const run_setup &run, int fd, medium_result &result, bool &engaged)
{
    ::asio::io_context io;
    plexus::discovery::static_discovery disc{{peer_card(run.ends.sub_name, run.ends.sub_id, run.ends.sub_port)}};
    pasio::same_host_transports ts{io};
    same_host_node node = ts.make_node(disc, run.ends.pub_id, run.same_host ? dial_opts(false) : distinct_fingerprint_opts(false, 0xB2B2B2B2u));
    if(run.same_host)
        REQUIRE_FALSE(node.router().local_fingerprint().is_null());
    node.listen({"tcp", "127.0.0.1:" + std::to_string(run.ends.pub_port)});
    const plexus::io::same_host_geometry geometry = companion_geometry();
    plexus::publisher<> companion_pub{node, run.fqn, companion_qos(), /*emit_source_identity=*/false, geometry};
    publish_paced(io, node, companion_pub, run, fd, result, engaged);
    if(!result.region_observed)
        result.region_observed = region_maps(run.fqn);
}

inline medium_result run_publisher(const run_setup &run, pid_t pid, int read_fd, int write_fd)
{
    ::close(write_fd);
    ::fcntl(read_fd, F_SETFL, ::fcntl(read_fd, F_GETFL, 0) | O_NONBLOCK);
    medium_result result;
    bool engaged = !run.same_host;
    publish(run, read_fd, result, engaged);
    const int status = reap(pid);
    take_tags(read_fd, result, engaged);
    ::close(read_fd);
    result.small_over_shm = run.c.shm_seen.load(std::memory_order_acquire) == 1u;
    (void)status;
    ::munmap(&run.c, sizeof(coord));
    return result;
}

inline medium_result one_run(const std::string &fqn, const std::string &small, const std::string &large, bool same_host)
{
    const run_endpoints ends = make_endpoints();
    coord *c                 = map_coord();
    if(c == nullptr)
        return {};
    int pipe_fd[2];
    if(::pipe(pipe_fd) != 0)
    {
        ::munmap(c, sizeof(coord));
        return {};
    }
    const pid_t pid = ::fork();
    if(pid < 0)
    {
        ::munmap(c, sizeof(coord));
        return {};
    }
    const run_setup run{fqn, small, large, same_host, ends, *c};
    if(pid == 0)
        run_subscriber(run, pipe_fd[0], pipe_fd[1]);
    return run_publisher(run, pid, pipe_fd[0], pipe_fd[1]);
}

}

#endif
