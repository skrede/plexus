#include "plexus/asio/same_host_transports.h"

#include "plexus/node.h"
#include "plexus/publisher.h"
#include "plexus/subscriber.h"
#include "plexus/node_options.h"

#include "plexus/shm/posix_shm_region_broker.h"
#include "plexus/shm/region_handle.h"
#include "plexus/shm/machine_fingerprint.h"

#include "plexus/io/shm/same_host.h"

#include "plexus/discovery/static_discovery.h"
#include "plexus/discovery/contact_card.h"

#include "plexus/io/reconnect_config.h"
#include "plexus/topic_qos.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <span>
#include <string>
#include <vector>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>

#include <fcntl.h>
#include <unistd.h>
#include <sys/wait.h>

// The whole-stack proof through the PUBLIC facade: two same_host_transports nodes,
// forked onto one host and rendezvousing over static_discovery, auto-engage a co-host
// shared-memory ring with NO platform conditional in the consumer code. The MEDIUM is
// asserted, not just delivery: while the nodes are live the parent attaches to the
// /dev/shm region named region_name_for(fqn) and REQUIREs it maps iff co-host. A
// forced-distinct-fingerprint variant proves the cross-host fail-closed path — delivery
// still crosses the TCP wire but no region exists. The byte-exact crossing is reproduced
// over >=2 independent runs with distinct per-run payloads (no success from a single run).

namespace pasio = plexus::asio;
namespace pio = plexus::io::shm;
using plexus::shm::posix_shm_region_broker;
using plexus::shm::region_handle;

namespace {

using same_host_node = decltype(std::declval<pasio::same_host_transports &>().make_node(
    std::declval<plexus::discovery::discovery &>(), std::declval<const plexus::node_id &>(),
    std::declval<plexus::node_options>()));

plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

plexus::node_options dial_opts(bool eager)
{
    plexus::node_options opts;
    opts.reconnect = plexus::io::reconnect_config{std::chrono::milliseconds(20),
                                                  std::chrono::milliseconds(500),
                                                  std::nullopt, std::nullopt};
    opts.redial_seed = 0xC0FFEEu;
    opts.dial_eagerly = eager;
    return opts;
}

// The peer's contact card pre-seeded into one side's static_discovery table: each forked
// process owns its OWN discovery, so the OTHER node's reachable tcp endpoint is supplied
// verbatim (no live mDNS) — browse-to-awareness then dials it deterministically.
plexus::discovery::service_info peer_card(const std::string &name, const plexus::node_id &id,
                                          std::uint16_t tcp_port)
{
    return {name, plexus::io::endpoint{"", "127.0.0.1"},
            plexus::discovery::assemble_contact_card(id, {{"tcp", tcp_port}})};
}

std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

// The control region the (fqn) request-direction ring maps over; the broker attach maps
// it read-only iff the facade auto-engaged the ring, so its region_status IS the medium.
std::string control_name(const std::string &fqn)
{
    return pio::region_name_for(fqn, pio::ring_direction::request);
}

// Pump until the predicate holds or the wall-clock backstop expires (a generous bound, not
// a tight deadline: the happy path returns the instant it converges).
template <typename Pred>
void pump_until(::asio::io_context &io, Pred pred, std::chrono::seconds bound)
{
    const auto deadline = std::chrono::steady_clock::now() + bound;
    while(!pred() && std::chrono::steady_clock::now() < deadline)
        io.poll();
}

bool region_maps(const std::string &fqn)
{
    posix_shm_region_broker broker;
    region_handle ctrl;
    return broker.attach(control_name(fqn), ctrl) == pio::region_status::ok;
}

// Force a distinct local fingerprint on each end so is_same_host fails — the cross-host
// fallback: the pair never reaches the shared-memory medium and stays on the wire.
plexus::node_options distinct_fingerprint_opts(bool eager, std::uint64_t fp)
{
    plexus::node_options opts = dial_opts(eager);
    opts.handshake.local_fingerprint = pio::host_fingerprint{fp};
    return opts;
}

// Fork two facade nodes over static_discovery and run ONE rendezvous. The child hosts the
// subscriber (eager dialer); the parent hosts the publisher (lazy acceptor). The parent
// drives demand to establish, observes the medium (the region maps iff co-host), publishes
// the run's distinct payload, and confirms the child received it byte-exact. Returns
// {delivered, region_observed}. `same_host` selects the co-host vs forced-distinct variant.
struct run_result
{
    bool delivered = false;
    bool region_observed = false;
};

run_result one_run(const std::string &fqn, const std::string &payload, bool same_host)
{
    // Compute the shared identity (ports, ids, names) in the PARENT before the fork so both
    // halves agree: the child inherits these via the forked address space (a post-fork
    // ::getpid() would differ between parent and child and de-sync the rendezvous).
    const std::uint16_t sub_port = 41000 + static_cast<std::uint16_t>(::getpid() % 2000);
    const std::uint16_t pub_port = sub_port + 1;
    const plexus::node_id sub_id = make_id(0x51);
    const plexus::node_id pub_id = make_id(0x52);
    const std::string sub_name = "node.sub." + std::to_string(::getpid());
    const std::string pub_name = "node.pub." + std::to_string(::getpid());

    int pipe_fd[2];
    if(::pipe(pipe_fd) != 0)
        return {};

    const pid_t pid = ::fork();
    if(pid < 0)
        return {};

    if(pid == 0)
    {
        ::close(pipe_fd[0]);
        bool got = false;
        {
            ::asio::io_context io;
            plexus::discovery::static_discovery disc{{peer_card(pub_name, pub_id, pub_port)}};
            pasio::same_host_transports ts{io};
            auto node = ts.make_node(disc, sub_id,
                                     same_host ? dial_opts(true)
                                               : distinct_fingerprint_opts(true, 0xA1A1A1A1u));

            std::string received;
            plexus::subscriber<> sub{node, fqn,
                                     [&](std::span<const std::byte> b) {
                                         received.assign(reinterpret_cast<const char *>(b.data()),
                                                         b.size());
                                     }};
            node.listen({"tcp", "127.0.0.1:" + std::to_string(sub_port)});
            pump_until(io, [&] { return node.router().is_connected(pub_id); },
                       std::chrono::seconds(10));
            pump_until(io, [&] { return received == payload; }, std::chrono::seconds(15));
            got = (received == payload);
        }
        const char b = got ? 1 : 0;
        const ssize_t wrote = ::write(pipe_fd[1], &b, 1);
        ::close(pipe_fd[1]);
        ::_exit(wrote == 1 && got ? 0 : 1);
    }

    ::close(pipe_fd[1]);
    // Make the child's done-signal pipe non-blocking so the parent can poll it between
    // republish turns without stalling on read.
    ::fcntl(pipe_fd[0], F_SETFL, ::fcntl(pipe_fd[0], F_GETFL, 0) | O_NONBLOCK);

    run_result result;
    {
        ::asio::io_context io;
        plexus::discovery::static_discovery disc{{peer_card(sub_name, sub_id, sub_port)}};
        pasio::same_host_transports ts{io};
        auto node = ts.make_node(disc, pub_id,
                                 same_host ? dial_opts(false)
                                           : distinct_fingerprint_opts(false, 0xB2B2B2B2u));

        // The fingerprint default-fill at the facade is non-null on a real host (the
        // null-guard convention) — only in the co-host variant where the default fill is in
        // effect (the forced-distinct variant overrides it).
        if(same_host)
            REQUIRE_FALSE(node.router().local_fingerprint().is_null());

        node.listen({"tcp", "127.0.0.1:" + std::to_string(pub_port)});

        plexus::topic_qos qos;
        qos.dispatch = pio::dispatch_hint::frequent; // a qualifying hint so the upgrade is eligible
        plexus::publisher<> pub{node, fqn, qos};

        // Drive to convergence: the accepting (publisher) side keys its inbound session by a
        // synthetic id, so the subscribe demand's arrival is observed by the medium itself
        // (the co-host ring maps once demand crosses 0->1) and by the child's delivery
        // signal. Republish each turn — once the subscribe demand reaches the forwarder the
        // fan delivers; the periodic republish closes the demand-arrival race deterministically.
        bool child_done = false;
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
        while(!child_done && std::chrono::steady_clock::now() < deadline)
        {
            io.poll();
            if(!result.region_observed && region_maps(fqn))
                result.region_observed = true;
            pub.publish(as_bytes(payload));
            char b = 0;
            if(::read(pipe_fd[0], &b, 1) == 1)
                child_done = (b == 1);
        }
        // A final medium sample while both nodes are still live (the co-host ring stays
        // mapped for the topic's life).
        if(!result.region_observed)
            result.region_observed = region_maps(fqn);
    }

    int status = 0;
    while(::waitpid(pid, &status, 0) < 0)
        ;
    char b = 0;
    if(::read(pipe_fd[0], &b, 1) == 1)
        result.delivered = (b == 1);
    else
        result.delivered = (WIFEXITED(status) && WEXITSTATUS(status) == 0);
    ::close(pipe_fd[0]);
    return result;
}

}

TEST_CASE("node_xproc_shm two co-host facade nodes auto-engage a shared-memory ring, the medium maps, looped",
          "[integration][shm][node_xproc_shm]")
{
    // The MEDIUM + byte-exact delivery over >=2 independent runs with DISTINCT payloads so
    // a stale region cannot pass: each run forks fresh and pid+run-scopes the fqn.
    bool any_region = false;
    for(int run = 0; run < 2; ++run)
    {
        const std::string fqn =
            "topic.xproc." + std::to_string(::getpid()) + "." + std::to_string(run);
        const std::string payload = "xproc-shm-run-" + std::to_string(run) + "-" +
                                    std::to_string(::getpid());

        const run_result r = one_run(fqn, payload, /*same_host=*/true);

        REQUIRE(r.delivered);              // byte-exact receipt on the subscriber
        REQUIRE(r.region_observed);        // the /dev/shm region mapped while the nodes were live
        REQUIRE_FALSE(region_maps(fqn));   // and is gone once both nodes tore down (no leak)
        any_region = any_region || r.region_observed;
    }
    REQUIRE(any_region);
}

TEST_CASE("node_xproc_shm a forced-distinct-fingerprint pair delivers over the wire with no region",
          "[integration][shm][node_xproc_shm]")
{
    // The cross-host fail-closed fallback: distinct fingerprints -> is_same_host false ->
    // no upgrade -> NO /dev/shm region, but the wire (TCP) still delivers byte-exact. The
    // wire is the always-present path, never suppressed.
    const std::string fqn = "topic.xproc.distinct." + std::to_string(::getpid());
    const std::string payload = "xproc-distinct-" + std::to_string(::getpid());

    const run_result r = one_run(fqn, payload, /*same_host=*/false);

    REQUIRE(r.delivered);                 // delivery over the wire survives the distinct fingerprints
    REQUIRE_FALSE(r.region_observed);     // and no co-host ring was engaged
    REQUIRE_FALSE(region_maps(fqn));
}
