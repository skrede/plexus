// A no-syscall perf rig over the in-process transport: two engines (A, B) on one
// bus, B's inbound session sink wired, both sides attached for fan-out on one
// topic. The steady loop is a pure publish on A -> frame codec -> wire forwarder
// -> bus enqueue -> bus deliver -> inbound demux -> frame router -> peer session
// -> B's message sink, driven to quiescence each iteration on an inline step
// executor. There is no socket, no syscall, no kernel time on the path, so a
// sampling profiler attributes 100% of the CPU to the pure plexus data path
// (engine / forwarders / demux map / frame codec). This is the transport-agnostic
// upper layer the differential profiling method isolates.
//
// It is a standalone binary, NOT a ctest: a profiler needs a long steady loop,
// not assertions. It is built directly into build-perf/ (RelWithDebInfo + frame
// pointers); it is not registered in the committed test tree, so the parity suite
// is untouched. The alloc-counter harness lets the rig also report the heap
// allocations the steady publish loop incurs after warmup — the empirical witness
// for the no-hot-path-allocation invariant on the pure upper layer.

#include "support/alloc_counter.h"

#include "plexus/io/known_peers.h"
#include "plexus/io/routing_engine.h"
#include "plexus/io/reconnect_config.h"
#include "plexus/io/handshake_fsm.h"

#include "plexus/inproc/inproc_timer.h"
#include "plexus/inproc/inproc_transport.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_bus.h"

#include "plexus/policy.h"

#include <span>
#include <chrono>
#include <cstdio>
#include <string>
#include <vector>
#include <cstdint>
#include <cstdlib>
#include <string_view>

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_timer;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_channel;
using plexus::inproc::inproc_transport;
using plexus::io::endpoint;
using plexus::io::handshake_fsm_config;
using plexus::io::reconnect_config;

namespace {

using clock_t_ = std::chrono::steady_clock;

// The hot-path Policy: an inline step executor + the in-process channel/timer over
// a real (steady) clock so the rig drives a genuine publish cadence, not virtual
// time. byte_owner is the default shared owner — the same handle the production
// asio policy selects, so the codec/forwarder allocation behavior matches.
struct rig_policy
{
    using executor_type     = inproc_executor<clock_t_> &;
    using byte_channel_type = inproc_channel<clock_t_>;
    using timer_type        = inproc_timer<clock_t_>;
    using byte_owner        = std::shared_ptr<const void>;

    static void post(executor_type ex, plexus::detail::move_only_function<void()> fn)
    {
        ex.post(std::move(fn));
    }
};

static_assert(plexus::Policy<rig_policy>);

using transport_t = inproc_transport<clock_t_>;
using engine      = plexus::io::routing_engine<rig_policy, transport_t, clock_t_>;

constexpr auto          k_long_timeout = std::chrono::hours(1);
constexpr std::uint64_t k_seed         = 0xC0FFEEu;

std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

handshake_fsm_config make_cfg(std::uint8_t id_seed)
{
    plexus::node_id id{};
    id[0] = std::byte{id_seed};
    return handshake_fsm_config{.self_id                  = id,
                                .version_major            = 1,
                                .version_minor            = 0,
                                .compatible_version_major = 1,
                                .compatible_version_minor = 0};
}

plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

reconnect_config forever_cfg()
{
    return reconnect_config{std::chrono::milliseconds(100), std::chrono::milliseconds(10000),
                            std::nullopt, std::nullopt};
}

// The two-node fan-out rendezvous: A and B over one bus (eager so awareness alone
// completes the handshake), B's inbound session sink wired to a sink counter, and
// both sides attached for fan-out on one topic so a publish on A flows to B.
struct rig
{
    inproc_bus<clock_t_>      bus;
    inproc_executor<clock_t_> ex{bus};
    transport_t               transport_a{ex, bus};
    transport_t               transport_b{ex, bus};
    plexus::log::null_logger  sink;
    engine                    a;
    engine                    b;

    plexus::node_id id_a{make_id(0xA1)};
    plexus::node_id id_b{make_id(0xB2)};
    endpoint        ep_a{"inproc", "node-a"};
    endpoint        ep_b{"inproc", "node-b"};

    std::uint64_t         received{0};
    engine::session_type *a_to_b{nullptr};

    rig()
            : a(transport_a, ex, make_cfg(0xA1), k_long_timeout, forever_cfg(), k_seed, sink,
                /*eager=*/true)
            , b(transport_b, ex, make_cfg(0xB2), k_long_timeout, forever_cfg(), k_seed, sink,
                /*eager=*/true)
    {
        a.listen(ep_a);
        b.listen(ep_b);
        a.note_peer(id_b, ep_b);
        ex.drain();
        a_to_b = a.session_for(id_b);

        plexus::node_id inbound = make_id(0x00);
        inbound[15]             = std::byte{1};
        auto *b_inbound         = b.session_for(inbound);
        b_inbound->on_message([this](std::string_view, std::span<const std::byte>) { ++received; });

        b.messages().attach_for_fanout(b_inbound->msg_peer(), "topic");
        a.messages().attach_for_fanout(a_to_b->msg_peer(), "topic");
        ex.drain();
    }

    // One steady publish: frame on A, fan to B's sink, drive to quiescence. This is
    // the single hot-path unit the profiler samples.
    void publish_once(const std::string &payload)
    {
        a.messages().publish("topic", as_bytes(payload), a_to_b->session_id());
        ex.drain();
    }
};

}

int main(int argc, char **argv)
{
    // Args: [iterations] [payload_bytes]. Defaults sized for a multi-second steady
    // window at this no-syscall throughput so perf gathers a stable sample set.
    const std::uint64_t iterations = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 20'000'000ull;
    const std::size_t   payload_bytes = argc > 2 ? std::strtoull(argv[2], nullptr, 10) : 64;

    rig               net;
    const std::string payload(payload_bytes, 'x');

    // Warmup: let the codec scratch buffers and the fan-out path reach steady-state
    // capacity so the measured window is the steady loop, not first-touch growth.
    for(int i = 0; i < 1000; ++i)
        net.publish_once(payload);

    const std::uint64_t baseline = net.received;
    plexus::testing::reset_alloc_count();

    const auto t0 = clock_t_::now();
    for(std::uint64_t i = 0; i < iterations; ++i)
        net.publish_once(payload);
    const auto t1 = clock_t_::now();

    const std::size_t   allocs     = plexus::testing::alloc_count();
    const std::uint64_t delivered  = net.received - baseline;
    const double        secs       = std::chrono::duration<double>(t1 - t0).count();
    const double        per_pub_ns = secs * 1e9 / static_cast<double>(iterations);
    const double        mpps       = static_cast<double>(iterations) / secs / 1e6;

    std::printf("inproc upper-layer rig: iterations=%llu payload=%zuB delivered=%llu\n",
                static_cast<unsigned long long>(iterations), payload_bytes,
                static_cast<unsigned long long>(delivered));
    std::printf("  wall=%.4fs  per_publish=%.2fns  throughput=%.3f Mpub/s\n", secs, per_pub_ns,
                mpps);
    std::printf("  steady-loop heap allocations=%zu  (%.6f allocs/publish)\n", allocs,
                static_cast<double>(allocs) / static_cast<double>(iterations));
    return delivered == iterations ? 0 : 1;
}
