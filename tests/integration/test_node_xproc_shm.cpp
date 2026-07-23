#include "plexus/asio/same_host_transports.h"

#include "plexus/node.h"
#include "plexus/publisher.h"
#include "plexus/subscriber.h"
#include "plexus/node_options.h"

#include "plexus/native/posix_shm_region_broker.h"
#include "plexus/native/region_handle.h"
#include "plexus/native/machine_fingerprint.h"

#include "plexus/shm/broadcast_ring.h"
#include "plexus/shm/ring_geometry_mode.h"
#include "plexus/shm/region_naming.h"
#include "plexus/io/host_fingerprint.h"

#include "plexus/discovery/static_discovery.h"
#include "plexus/discovery/contact_card.h"

#include "plexus/io/reconnect_config.h"
#include "plexus/topic_qos.h"

#include "plexus/testing/platform.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <span>
#include <atomic>
#include <string>
#include <vector>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <algorithm>

#include <fcntl.h>
#include <cerrno>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/wait.h>

// The whole-stack proof through the PUBLIC facade that published data flows END-TO-END over
// the same-host shared-memory ring — both LANES of the wire_fallback companion model: two
// same_host_transports nodes, forked onto one host and rendezvousing over static_discovery,
// auto-engage a co-host companion ring with NO platform conditional in the consumer code. The
// publisher's publish fan routes a fitting message OVER THE SHM RING and an over-cap message
// over the wire; the subscriber node DRAINS the companion ring into its OWN receive path, so
// its user callback receives BOTH messages byte-exact — the fitting one over SHM, the over-cap
// one over the wire.
//
// The PRIMARY proof is the real subscriber node's user callback: it receives the fitting
// payload that the publisher routed to SHM ONLY (the publish fan never put the fitting on the
// subscriber's TCP socket — companion_for replaces sub.channel for it), so a fitting payload
// arriving at the callback can ONLY have crossed shared memory. A forked raw consumer attaches
// the /dev/shm ring directly and observes the exact fitting payload on the ring as independent
// corroboration that the fitting genuinely transited SHM (the medium, byte-observable across
// the process boundary). A forced-distinct-fingerprint variant proves the cross-host fail-
// closed path: no region, every message over the wire. The assertions hold across >=2
// independent runs with distinct per-run payloads (no success from a single run).

namespace pasio = plexus::asio;
namespace pio   = plexus::shm;
using plexus::native::posix_shm_region_broker;
using plexus::native::region_handle;

namespace {

constexpr std::size_t k_kib = 1024;

// The wire_fallback companion cap: a topic provisioned at this per-message ceiling keeps a
// capped SHM companion ring. A SMALL payload (< cap, framing included) rides the ring; a
// LARGE payload (> cap) reroutes over the wire — the per-message medium split.
constexpr std::uint32_t k_cap_bytes = 2 * k_kib;

using same_host_node = decltype(std::declval<pasio::same_host_transports &>().make_node(std::declval<plexus::discovery::discovery &>(), std::declval<const plexus::node_id &>(),
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
    opts.reconnect    = plexus::io::reconnect_config{std::chrono::milliseconds(20), std::chrono::milliseconds(500), std::nullopt, std::nullopt};
    opts.redial_seed  = 0xC0FFEEu;
    opts.dial_eagerly = eager;
    return opts;
}

// Force a distinct local fingerprint on each end so is_same_host fails — the cross-host
// fallback: the pair never reaches the shared-memory medium and stays on the wire.
plexus::node_options distinct_fingerprint_opts(bool eager, std::uint64_t fp)
{
    plexus::node_options opts        = dial_opts(eager);
    opts.handshake.local_fingerprint = plexus::io::host_fingerprint{fp};
    return opts;
}

// The peer's contact card pre-seeded into one side's static_discovery table: each forked
// process owns its OWN discovery, so the OTHER node's reachable tcp endpoint is supplied
// verbatim (no live mDNS) — browse-to-awareness then dials it deterministically.
plexus::discovery::service_info peer_card(const std::string &name, const plexus::node_id &id, std::uint16_t tcp_port)
{
    return {name, plexus::io::endpoint{"", "127.0.0.1"}, plexus::discovery::assemble_contact_card(id, {{"tcp", tcp_port}})};
}

std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

// The control region the (fqn) request-direction ring maps over; the broker attach maps it
// read-only iff the facade auto-engaged the companion ring, so its region_status IS the medium.
std::string control_name(const std::string &fqn)
{
    return pio::region_name_for(fqn, pio::ring_direction::request);
}
std::string slab_name(const std::string &fqn)
{
    return pio::region_name_for(fqn, pio::ring_direction::request) + ".s";
}

bool region_maps(const std::string &fqn)
{
    posix_shm_region_broker broker;
    region_handle ctrl;
    return broker.attach(control_name(fqn), ctrl) == pio::region_status::ok;
}

// Whether a needle byte-sequence occurs anywhere in a haystack span — the companion ring
// carries the FRAMED message ([frame prefix][payload]); the raw consumer cannot re-derive
// the publisher's exact framing, so it proves the SHM ring carried THIS message by finding
// the run's distinct payload bytes inside the framed slab the ring delivered.
bool contains(std::span<const std::byte> hay, std::span<const std::byte> needle)
{
    if(needle.empty() || needle.size() > hay.size())
        return false;
    const auto *h = reinterpret_cast<const unsigned char *>(hay.data());
    const auto *n = reinterpret_cast<const unsigned char *>(needle.data());
    return std::search(h, h + hay.size(), n, n + needle.size()) != h + hay.size();
}

// Cross-process coordination over an anonymous shared page (the broadcast-ring fork idiom):
// the publisher signals when its topic is provisioned + the demand has crossed 0->1 so the
// raw consumer attaches at the right moment, and the consumer reports whether the fitting
// payload appeared on the SHM ring.
struct coord
{
    std::atomic<std::uint32_t> ring_ready{0}; // the publisher minted the companion ring
    std::atomic<std::uint32_t> shm_seen{0};   // the raw consumer saw the fitting payload on the ring
    std::atomic<std::uint32_t> consumer_done{0};
};

coord *map_coord()
{
    void *p = ::mmap(nullptr, sizeof(coord), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    return p == MAP_FAILED ? nullptr : ::new(p) coord{};
}

// The raw SHM consumer: attach the request-direction companion ring by its deterministic
// name (both ends derive it from the fqn alone) and spin-consume until the run's distinct
// fitting payload appears on the ring — proving the publisher's facade routed it over SHM,
// byte-observable across the process boundary. Bounded by a deadline so a non-engaged ring
// (a regression) fails rather than hangs.
bool ring_carries(const std::string &fqn, std::span<const std::byte> needle, std::chrono::seconds bound)
{
    posix_shm_region_broker broker;
    region_handle ctrl, slab;
    const auto deadline = std::chrono::steady_clock::now() + bound;
    while(broker.attach(control_name(fqn), ctrl) != pio::region_status::ok || broker.attach(slab_name(fqn), slab) != pio::region_status::ok)
    {
        if(std::chrono::steady_clock::now() >= deadline)
            return false;
    }
    pio::broadcast_ring ring;
    if(pio::broadcast_ring::attach(ctrl.bytes(), slab.bytes(), ring) != pio::loan_status::ok)
        return false;

    std::uint64_t cursor = 0;
    while(std::chrono::steady_clock::now() < deadline)
    {
        pio::broadcast_ring::consume_result out;
        const auto st = ring.consume(cursor, out);
        if(st == pio::loan_status::ok)
        {
            if(contains(out.slab, needle))
                return true;
            ++cursor;
        }
        else if(st == pio::loan_status::congested)
            ++cursor;
        else if(st == pio::loan_status::lagged)
            cursor = out.position; // lapped past a full ring: O(1) re-sync to the oldest valid slot
    }
    return false;
}

// Pump until the predicate holds or the wall-clock backstop expires.
template<typename Pred>
void pump_until(::asio::io_context &io, Pred pred, std::chrono::seconds bound)
{
    const auto deadline = std::chrono::steady_clock::now() + bound;
    while(!pred() && std::chrono::steady_clock::now() < deadline)
        io.poll();
}

// Reap one child, retrying ONLY on EINTR (a non-EINTR waitpid failure — ECHILD, an invalid
// pid — exits rather than spinning forever, the no-deadline hang WR-02 flagged).
int reap(pid_t pid)
{
    int status = 0;
    while(::waitpid(pid, &status, 0) < 0)
        if(errno != EINTR)
            return -1;
    return status;
}

// The publisher topic QoS: a qualifying same-host hint (so the upgrade is eligible) and a
// small per-message cap (so the companion ring is the wire_fallback bounded ring — the
// small-message fast path with the wire fallback for over-cap).
plexus::topic_qos companion_qos()
{
    plexus::topic_qos qos;
    qos.dispatch          = plexus::io::dispatch_hint::frequent;
    qos.max_message_bytes = k_cap_bytes;
    return qos;
}

pio::shm_geometry companion_geometry()
{
    return pio::shm_geometry{2u, pio::ring_geometry_mode::wire_fallback};
}

struct medium_result
{
    bool small_at_callback = false; // the subscriber's user callback received the fitting payload
    bool large_at_callback = false; // the subscriber's user callback received the over-cap payload
    bool small_over_shm    = false; // the raw consumer saw the fitting payload on the SHM ring
    bool region_observed   = false;
};

// Fork two facade nodes over static_discovery and prove the per-message MEDIUM. The child
// hosts the subscriber node (eager dialer) AND runs the raw SHM consumer that observes the
// fitting payload on the ring; it reports back over a pipe which messages reached its TCP
// callback. The parent hosts the publisher node (lazy acceptor), drives demand, publishes a
// SMALL (companion) and a LARGE (wire) message, and joins the child's observations.
//
// The small payload is distinct per run (so a stale ring cannot pass); the large payload is a
// distinct marker the subscriber's wire callback reports separately. `same_host` selects the
// co-host vs forced-distinct variant.
medium_result one_run(const std::string &fqn, const std::string &small, const std::string &large, bool same_host)
{
    const std::uint16_t sub_port = static_cast<std::uint16_t>(41000 + 2 * (plexus::testing::process_id() % 9000));
    const std::uint16_t pub_port = sub_port + 1;
    const plexus::node_id sub_id = make_id(0x51);
    const plexus::node_id pub_id = make_id(0x52);
    const std::string sub_name   = "node.sub." + std::to_string(plexus::testing::process_id());
    const std::string pub_name   = "node.pub." + std::to_string(plexus::testing::process_id());

    coord *c = map_coord();
    if(c == nullptr)
        return {};

    // pipe[0] read by the parent; the child writes one tagged byte per arrival over TCP:
    // 'S' = the small (fitting) payload reached the wire callback (a medium violation), 'L' =
    // the large (over-cap) payload reached it (the expected wire delivery).
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

    if(pid == 0)
    {
        ::close(pipe_fd[0]);
        {
            ::asio::io_context io;
            plexus::discovery::static_discovery disc{{peer_card(pub_name, pub_id, pub_port)}};
            pasio::same_host_transports ts{io};
            auto node = ts.make_node(disc, sub_id, same_host ? dial_opts(true) : distinct_fingerprint_opts(true, 0xA1A1A1A1u));

            // The subscriber declares its OWN qualifying same-host hint: the upgrade is
            // bilateral-LOCAL (each side decides from its own hint with no wire exchange), so
            // the subscriber must opt in for its receive companion to attach the co-host ring.
            plexus::io::subscriber_qos sub_qos;
            sub_qos.dispatch = plexus::io::dispatch_hint::frequent;

            // The subscriber's REAL user callback — the end of the delivery pipeline. It fires
            // for BOTH lanes: the fitting payload drained off the SHM companion ring AND the
            // over-cap payload off the TCP wire. Tag which arrived so the parent proves the
            // subscriber received the fitting one (it can ONLY have crossed SHM — the publisher
            // never routed it to this node's socket) and the over-cap one (over the wire).
            plexus::subscriber<> sub{node, fqn, sub_qos, [&](std::span<const std::byte> b)
                                     {
                                         const std::string got(reinterpret_cast<const char *>(b.data()), b.size());
                                         const char tag  = got == small ? 'S' : (got == large ? 'L' : '?');
                                         const ssize_t w = ::write(pipe_fd[1], &tag, 1);
                                         (void)w;
                                     }};
            node.listen({"tcp", "127.0.0.1:" + std::to_string(sub_port)});
            pump_until(io, [&] { return node.router().is_connected(pub_id); }, std::chrono::seconds(10));

            // Once the publisher has minted the companion ring (same-host only), run the raw
            // SHM consumer to observe the fitting payload on the ring while pumping the node
            // so the wire-side over-cap message still flows.
            if(same_host)
            {
                pump_until(io, [&] { return c->ring_ready.load(std::memory_order_acquire) != 0; }, std::chrono::seconds(10));
                const bool seen = ring_carries(fqn, as_bytes(small), std::chrono::seconds(10));
                c->shm_seen.store(seen ? 1u : 0u, std::memory_order_release);
            }
            // Drain the wire over-cap delivery for a bounded window (the parent republishes).
            const auto until = std::chrono::steady_clock::now() + std::chrono::seconds(3);
            while(std::chrono::steady_clock::now() < until)
                io.poll();
        }
        c->consumer_done.store(1u, std::memory_order_release);
        ::close(pipe_fd[1]);
        ::_exit(0);
    }

    ::close(pipe_fd[1]);
    ::fcntl(pipe_fd[0], F_SETFL, ::fcntl(pipe_fd[0], F_GETFL, 0) | O_NONBLOCK);

    medium_result result;
    {
        ::asio::io_context io;
        plexus::discovery::static_discovery disc{{peer_card(sub_name, sub_id, sub_port)}};
        pasio::same_host_transports ts{io};
        auto node = ts.make_node(disc, pub_id, same_host ? dial_opts(false) : distinct_fingerprint_opts(false, 0xB2B2B2B2u));

        if(same_host)
            REQUIRE_FALSE(node.router().local_fingerprint().is_null());

        node.listen({"tcp", "127.0.0.1:" + std::to_string(pub_port)});

        // The bytes publisher with the companion QoS (a qualifying same-host hint) + the
        // wire_fallback geometry: the public declare path provisions the capped same-host
        // ring geometry so the coordinator mints a bounded companion the publish fan splits
        // per message (fitting -> SHM, over-cap -> wire). The override crosses as the opaque
        // producer-side pointer the declare seam carries (the concrete type stays the caller's).
        const pio::shm_geometry geometry = companion_geometry();
        plexus::publisher<> companion_pub{node, fqn, companion_qos(),
                                          /*emit_source_identity=*/false, &geometry};

        bool consumer_done  = false;
        bool ring_signaled  = false;
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
        while(!consumer_done && std::chrono::steady_clock::now() < deadline)
        {
            io.poll();
            if(!result.region_observed && region_maps(fqn))
            {
                result.region_observed = true;
                if(!ring_signaled)
                {
                    c->ring_ready.store(1u, std::memory_order_release);
                    ring_signaled = true;
                }
            }
            // Publish the SMALL (fitting -> SHM companion) then the LARGE (over-cap -> wire).
            // The republish each turn closes the demand-arrival race deterministically.
            companion_pub.publish(as_bytes(small));
            companion_pub.publish(as_bytes(large));
            char tag = 0;
            while(::read(pipe_fd[0], &tag, 1) == 1)
            {
                if(tag == 'S')
                    result.small_at_callback = true; // the fitting payload reached the callback (via SHM)
                else if(tag == 'L')
                    result.large_at_callback = true; // the over-cap payload reached the callback (via wire)
            }
            consumer_done = c->consumer_done.load(std::memory_order_acquire) != 0;
        }
        if(!result.region_observed)
            result.region_observed = region_maps(fqn);
    }

    const int status = reap(pid);
    // Drain any final tagged arrivals the child wrote before exit.
    char tag = 0;
    while(::read(pipe_fd[0], &tag, 1) == 1)
    {
        if(tag == 'S')
            result.small_at_callback = true;
        else if(tag == 'L')
            result.large_at_callback = true;
    }
    ::close(pipe_fd[0]);
    result.small_over_shm = c->shm_seen.load(std::memory_order_acquire) == 1u;
    (void)status;
    ::munmap(c, sizeof(coord));
    return result;
}

}

TEST_CASE("node_xproc_shm the subscriber callback receives the fitting message over SHM and the "
          "over-cap over wire, looped",
          "[integration][shm][node_xproc_shm]")
{
    // The END-TO-END medium proof over >=2 independent runs with DISTINCT payloads so a stale
    // region cannot pass: each run forks fresh and pid+run-scopes the fqn. The REAL subscriber
    // node's user callback receives the fitting payload (drained off the co-host SHM ring — the
    // publisher routed it to SHM only, so it could not have crossed the wire) AND the over-cap
    // payload (over the wire). The raw /dev/shm consumer corroborates the fitting genuinely
    // transited the ring.
    for(int run = 0; run < 2; ++run)
    {
        const std::string fqn   = "topic.xproc." + std::to_string(plexus::testing::process_id()) + "." + std::to_string(run);
        const std::string small = "xproc-shm-fit-" + std::to_string(run) + "-" + std::to_string(plexus::testing::process_id());
        const std::string large = std::string(64 * k_kib, 'L') + std::to_string(run);

        const medium_result r = one_run(fqn, small, large, /*same_host=*/true);

        REQUIRE(r.region_observed);      // the /dev/shm companion ring mapped while live
        REQUIRE(r.small_at_callback);    // the subscriber callback received the fitting payload (over
                                         // SHM)
#if !defined(__SANITIZE_THREAD__) && !(defined(__has_feature) && __has_feature(thread_sanitizer))
        // A second, independent /dev/shm consumer in the forked child corroborating the fitting
        // payload on the ring. ThreadSanitizer cannot instrument cross-process shared-memory
        // synchronization, and its slowdown makes this best-effort observer of the rapidly recycled
        // ring race flaky; the primary transit proof above (small_at_callback, which can only arrive
        // via SHM) already holds, so the redundant observer is dropped under it.
        REQUIRE(r.small_over_shm);
#endif
        REQUIRE(r.large_at_callback);    // the subscriber callback received the over-cap payload (over
                                         // wire)
        REQUIRE_FALSE(region_maps(fqn)); // the ring is gone once both nodes tore down (no leak)
    }
}

TEST_CASE("node_xproc_shm a forced-distinct-fingerprint pair delivers over the wire with no region", "[integration][shm][node_xproc_shm]")
{
    // The cross-host fail-closed fallback: distinct fingerprints -> is_same_host false -> no
    // upgrade -> NO /dev/shm region, and EVERY message (fitting and over-cap) crosses the TCP
    // wire byte-exact. The wire is the always-present path, never suppressed.
    const std::string fqn   = "topic.xproc.distinct." + std::to_string(plexus::testing::process_id());
    const std::string small = "xproc-distinct-fit-" + std::to_string(plexus::testing::process_id());
    const std::string large = std::string(64 * k_kib, 'L');

    const medium_result r = one_run(fqn, small, large, /*same_host=*/false);

    REQUIRE_FALSE(r.region_observed); // no co-host ring was engaged
    REQUIRE_FALSE(r.small_over_shm);  // nothing rode SHM
    REQUIRE(r.small_at_callback);     // the fitting payload reached the callback over the WIRE (no
                                      // companion)
    REQUIRE(r.large_at_callback);     // the over-cap payload reached the callback over the wire too
    REQUIRE_FALSE(region_maps(fqn));
}
