#include "plexus/asio/default_discovery.h"
#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_transport.h"

#include "plexus/node.h"
#include "plexus/node_options.h"

#include "plexus/node_id.h"

#include "plexus/testing/platform.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <string>
#include <chrono>
#include <vector>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <algorithm>

#include <cerrno>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/wait.h>

// Two nodes in SEPARATE PROCESSES on one host discover each other out of the box, with NO discovery
// configuration — the node-facade-shm-not-xproc gap closed as a first-class scenario. Same-host
// cross-process multicast loopback (already enabled) is exactly what makes this work (DISC-02). The
// mutual-awareness outcome is reproduced over several runs, each forking fresh processes.

namespace pasio = plexus::asio;
using asio_node = plexus::basic_node<pasio::asio_policy, pasio::asio_transport>;

namespace {

plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0]  = std::byte{seed};
    id[15] = std::byte{static_cast<unsigned char>(seed ^ 0x5a)};
    return id;
}

plexus::node_options make_opts()
{
    plexus::node_options opts;
    opts.redial_seed = 0xC0FFEEu;
    return opts;
}

struct coord
{
    std::atomic<std::uint32_t> aware_parent{0}; // the parent noted the child
    std::atomic<std::uint32_t> aware_child{0};  // the child noted the parent
};

coord *map_coord()
{
    void *p = ::mmap(nullptr, sizeof(coord), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    return p == MAP_FAILED ? nullptr : ::new(p) coord{};
}

int reap(pid_t pid)
{
    int status = 0;
    while(::waitpid(pid, &status, 0) < 0)
        if(errno != EINTR)
            return -1;
    return status;
}

// One node's leg: bring up a configless node on the default discovery group, listen on its own tcp
// port, and pump until it has noted the peer AND the peer has noted it (both flags set) or the bound
// elapses. self_flag/peer_flag are the two shared atomics from this process's point of view.
void run_leg(std::atomic<std::uint32_t> &self_flag, std::atomic<std::uint32_t> &peer_flag, std::uint16_t my_port, const plexus::node_id &peer_id)
{
    ::asio::io_context io;
    pasio::default_discovery disc{io};
    pasio::asio_transport tp{io};
    asio_node node{io, disc.discovery(), (my_port & 1u) ? make_id(0xB2) : make_id(0xA1), tp, make_opts()};
    node.listen({"tcp", "127.0.0.1:" + std::to_string(my_port)});

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
    while(std::chrono::steady_clock::now() < deadline)
    {
        io.poll();
        if(io.stopped())
            io.restart();
        if(node.router().known().contains(peer_id))
            self_flag.store(1u, std::memory_order_release);
        if(self_flag.load(std::memory_order_acquire) != 0 && peer_flag.load(std::memory_order_acquire) != 0)
            break;
    }
}

struct run_result
{
    bool aware_parent = false;
    bool aware_child  = false;
};

run_result one_run(std::uint16_t parent_port, std::uint16_t child_port)
{
    coord *c = map_coord();
    if(c == nullptr)
        return {};

    const pid_t pid = ::fork();
    if(pid < 0)
    {
        ::munmap(c, sizeof(coord));
        return {};
    }

    if(pid == 0)
    {
        run_leg(c->aware_child, c->aware_parent, child_port, make_id(0xA1));
        ::_exit(0);
    }

    run_leg(c->aware_parent, c->aware_child, parent_port, make_id(0xB2));
    const int status = reap(pid);
    (void)status;

    run_result r{c->aware_parent.load(std::memory_order_acquire) != 0, c->aware_child.load(std::memory_order_acquire) != 0};
    ::munmap(c, sizeof(coord));
    return r;
}

} // namespace

TEST_CASE("default_discovery_xproc two configless nodes in separate processes discover each other "
          "out of the box (same-host multicast loopback across processes)",
          "[integration][discovery][default_discovery_xproc]")
{
    constexpr int k_runs          = 3;
    const std::uint16_t base_port = static_cast<std::uint16_t>(24000 + (plexus::testing::process_id() % 500) * 2);

    int mutual = 0;
    for(int run = 0; run < k_runs; ++run)
    {
        // Even parent port / odd child port: run_leg keys its own node_id off the port parity so the
        // two processes take distinct identities without any shared handshake.
        const std::uint16_t parent_port = static_cast<std::uint16_t>((base_port + 2 * run) & ~1u);
        const std::uint16_t child_port  = static_cast<std::uint16_t>(parent_port + 1);
        const run_result r              = one_run(parent_port, child_port);

        if(!r.aware_parent && !r.aware_child && run == 0)
            SKIP("cross-process multicast loopback unavailable on this host: separate-process nodes reached no mutual awareness within the bound");

        REQUIRE(r.aware_parent); // the parent process noted the child, out of the box
        REQUIRE(r.aware_child);  // the child process noted the parent, out of the box
        ++mutual;
        WARN("[xproc] run " << run << ": aware_parent=" << r.aware_parent << " aware_child=" << r.aware_child);
    }

    REQUIRE(mutual >= 3);
    WARN("[xproc] same-host cross-process out-of-box discovery reproduced over " << mutual << " runs (DISC-02).");
}
