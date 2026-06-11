// A no-syscall perf rig for the typed in-process pub/sub fast path. Two nodes on one bus
// (single-dialer), one typed topic; the steady loop borrows a pool slot, publishes the
// object, and drives to quiescence so the object rides the zero-serialization lane to the
// typed callback by reference. It reports allocs/publish and ns/publish for:
//   (a) the fast path (a sized pool, the object lane), and
//   (b) a forced fallback (a depth-0 pool, the serialize path),
// and sweeps the pool depth across {2,4,8,16,32} to substantiate (or correct) the typed
// publisher's default depth empirically — the no-felt-numbers discipline.
//
// It is a standalone binary, NOT a ctest: a perf measurement needs a long steady loop and
// reports numbers, it does not assert. The alloc-counter harness (one TU per executable)
// lets it report the steady-loop heap behavior directly.

#include "support/alloc_counter.h"

#include "plexus/node.h"
#include "plexus/publisher.h"
#include "plexus/subscriber.h"
#include "plexus/expected.h"
#include "plexus/typed_codec.h"
#include "plexus/wire_bytes.h"
#include "plexus/node_options.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/discovery/static_discovery.h"

#include <span>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <memory>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <optional>
#include <system_error>

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_transport;
using plexus::discovery::static_discovery;

namespace {

using clock_t_ = std::chrono::steady_clock;
using inproc_node = plexus::node<inproc_policy, inproc_transport<>>;

struct sample
{
    std::uint32_t value{};
};

struct counting_codec
{
    using value_type = sample;

    std::shared_ptr<std::atomic<int>> encodes = std::make_shared<std::atomic<int>>(0);

    plexus::wire_bytes<> encode(const sample &v) const
    {
        ++*encodes;
        auto owner = std::make_shared<std::vector<std::byte>>(4);
        for(int i = 0; i < 4; ++i)
            (*owner)[i] = static_cast<std::byte>((v.value >> (8 * i)) & 0xff);
        std::span<const std::byte> view{owner->data(), owner->size()};
        return plexus::wire_bytes<>{view, std::move(owner)};
    }

    plexus::expected<void, std::error_code> decode(std::span<const std::byte> bytes, sample &out) const
    {
        if(bytes.size() != 4)
            return plexus::expected<void, std::error_code>{
                plexus::unexpect, std::make_error_code(std::errc::invalid_argument)};
        std::uint32_t v = 0;
        for(int i = 0; i < 4; ++i)
            v |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(bytes[i])) << (8 * i);
        out.value = v;
        return {};
    }

    plexus::type_identity type_info() const { return {0xABCD1234u, "sample"}; }
};

using typed_publisher = plexus::publisher<inproc_policy, counting_codec>;
using typed_subscriber = plexus::subscriber<inproc_policy, counting_codec>;

plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

plexus::node_options make_opts(bool eager)
{
    plexus::node_options opts;
    opts.reconnect = plexus::io::reconnect_config{std::chrono::milliseconds(50),
                                                  std::chrono::milliseconds(2000),
                                                  std::nullopt, std::nullopt};
    opts.redial_seed = 0x9163u;
    opts.dial_eagerly = eager;
    return opts;
}

struct net
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    inproc_transport<> ta{ex, bus};
    inproc_transport<> tb{ex, bus};
    static_discovery disc{{}};

    inproc_node a{ex, disc, make_id(0x0A), ta, make_opts(/*eager=*/true)};
    inproc_node b{ex, disc, make_id(0x0B), tb, make_opts(/*eager=*/false)};

    void drive() { ex.drain(); }

    void connect()
    {
        a.listen({"inproc", "host-a:5000"});
        b.listen({"inproc", "host-b:6000"});
        drive();
    }
};

struct result
{
    double  per_pub_ns;
    double  allocs_per_pub;
    std::uint64_t delivered;
};

// One fast-path run at a given pool depth: borrow -> publish(loan&&) -> drain, looped.
result run_fast(std::size_t pool_depth, std::uint64_t iterations, std::uint64_t warm)
{
    net n;
    n.connect();

    std::uint64_t delivered = 0;
    typed_subscriber s{n.a, "topic", [&](const sample &) { ++delivered; }};
    plexus::typed_publisher_options popts;
    popts.pool_depth = pool_depth;
    typed_publisher p{n.b, "topic", popts, counting_codec{}};
    n.drive();

    auto publish_once = [&](std::uint32_t v) {
        auto loan = p.borrow();
        if(loan)
        {
            loan->value = v;
            p.publish(std::move(loan));
        }
        else
        {
            p.publish(sample{v});
        }
        n.drive();
    };

    for(std::uint64_t i = 0; i < warm; ++i)
        publish_once(static_cast<std::uint32_t>(i));

    const std::uint64_t baseline = delivered;
    plexus::testing::reset_alloc_count();
    const auto t0 = clock_t_::now();
    for(std::uint64_t i = 0; i < iterations; ++i)
        publish_once(static_cast<std::uint32_t>(i));
    const auto t1 = clock_t_::now();
    const std::size_t allocs = plexus::testing::alloc_count();

    const double secs = std::chrono::duration<double>(t1 - t0).count();
    return {secs * 1e9 / static_cast<double>(iterations),
            static_cast<double>(allocs) / static_cast<double>(iterations),
            delivered - baseline};
}

// The forced fallback (depth-0 pool): every publish(const T&) serializes through the codec.
result run_fallback(std::uint64_t iterations, std::uint64_t warm)
{
    net n;
    n.connect();

    std::uint64_t delivered = 0;
    typed_subscriber s{n.a, "topic", [&](const sample &) { ++delivered; }};
    plexus::typed_publisher_options popts;
    popts.pool_depth = 0;
    typed_publisher p{n.b, "topic", popts, counting_codec{}};
    n.drive();

    auto publish_once = [&](std::uint32_t v) { p.publish(sample{v}); n.drive(); };

    for(std::uint64_t i = 0; i < warm; ++i)
        publish_once(static_cast<std::uint32_t>(i));

    const std::uint64_t baseline = delivered;
    plexus::testing::reset_alloc_count();
    const auto t0 = clock_t_::now();
    for(std::uint64_t i = 0; i < iterations; ++i)
        publish_once(static_cast<std::uint32_t>(i));
    const auto t1 = clock_t_::now();
    const std::size_t allocs = plexus::testing::alloc_count();

    const double secs = std::chrono::duration<double>(t1 - t0).count();
    return {secs * 1e9 / static_cast<double>(iterations),
            static_cast<double>(allocs) / static_cast<double>(iterations),
            delivered - baseline};
}

}

int main(int argc, char **argv)
{
    const std::uint64_t iterations = argc > 1 ? std::strtoull(argv[1], nullptr, 10) : 2'000'000ull;
    const std::uint64_t warm = 2000;

    std::printf("typed inproc rig: iterations=%llu (no syscalls, single executor)\n",
                static_cast<unsigned long long>(iterations));
    std::printf("  NOTE: the host has no CPU exclusivity; ns figures are indicative, "
                "alloc figures are exact.\n\n");

    const result fb = run_fallback(iterations, warm);
    std::printf("  forced fallback (serialize): per_publish=%.1fns  allocs/publish=%.4f  delivered=%llu\n\n",
                fb.per_pub_ns, fb.allocs_per_pub, static_cast<unsigned long long>(fb.delivered));

    std::printf("  pool-depth sweep (fast path):\n");
    std::printf("    %6s  %14s  %16s\n", "depth", "per_publish(ns)", "allocs/publish");
    const std::size_t depths[] = {2, 4, 8, 16, 32};
    for(std::size_t d : depths)
    {
        const result r = run_fast(d, iterations, warm);
        std::printf("    %6zu  %14.1f  %16.4f%s\n", d, r.per_pub_ns, r.allocs_per_pub,
                    r.delivered == iterations ? "" : "  [DELIVERY MISMATCH]");
    }
    return 0;
}
