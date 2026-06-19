// The borrowed-executor named gate, shaped like the vagus integration bridge: a host
// struct OWNS the whole substrate — the executor, the discovery service, and the
// transports — and embeds two plexus::node instances that BORROW all three. Nothing
// inside a node owns an io_context or a thread; the host alone pumps the loop. Three
// assertion families prove it:
//   1. No thread spawned: on Linux, the /proc/self/task count is unchanged across node
//      construction and a full pub/sub + call round-trip (the structural property is
//      platform-independent; the proof is Linux).
//   2. No progress without the host pump: a publish before any pump delivers nothing;
//      delivery happens only once the host steps the executor — sampled at several
//      pipeline stages (after construction, after subscribe, after publish).
//   3. The lifetime contract: the substrate the nodes borrow outlives them by member
//      order (declared before the node refs), the discipline that keeps a borrowed-
//      executor node from ever driving its own loop.

#include "plexus/node.h"
#include "plexus/caller.h"
#include "plexus/procedure.h"
#include "plexus/publisher.h"
#include "plexus/subscriber.h"
#include "plexus/node_options.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/wire/rpc_status.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <vector>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>

#ifdef __linux__
    #include <filesystem>
#endif

namespace {

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_transport;
using plexus::discovery::static_discovery;

using inproc_node       = plexus::node<inproc_policy, inproc_transport<>>;
using inproc_publisher  = plexus::publisher<>;
using inproc_subscriber = plexus::subscriber<>;
using inproc_caller     = plexus::caller<>;
using inproc_procedure  = plexus::procedure<>;
using reply_t           = plexus::reply;

plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

plexus::node_options make_opts(bool eager)
{
    plexus::node_options opts;
    opts.reconnect    = plexus::io::reconnect_config{std::chrono::milliseconds(50),
                                                     std::chrono::milliseconds(2000), std::nullopt,
                                                     std::nullopt};
    opts.redial_seed  = 0xB0D6E5u;
    opts.dial_eagerly = eager;
    return opts;
}

std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

std::string to_string(std::span<const std::byte> b)
{
    return std::string{reinterpret_cast<const char *>(b.data()), b.size()};
}

#ifdef __linux__
// The live OS thread count of this process, read from the kernel's per-task directory.
// A node that secretly spawned a runtime thread would bump this; a borrowed-executor node
// never does.
std::size_t os_thread_count()
{
    std::size_t n = 0;
    for(const auto &entry : std::filesystem::directory_iterator("/proc/self/task"))
    {
        (void)entry;
        ++n;
    }
    return n;
}
#endif

template<typename Pred>
bool pump_until(inproc_executor<> &ex, Pred done, int step_budget = 100000)
{
    for(int i = 0; i < step_budget; ++i)
    {
        if(done())
            return true;
        if(!ex.step())
            return done();
    }
    return done();
}

// The vagus-shaped bridge: the HOST owns the io_context-equivalent (executor + bus), the
// discovery service, and the transports. The two embedded nodes borrow all three. Member
// ORDER is load-bearing: the substrate is declared BEFORE the node refs so it outlives
// them (reverse destruction order) — the living documentation of the borrowed-executor
// lifetime contract.
struct host_bridge
{
    inproc_bus<>       bus;
    inproc_executor<>  ex{bus};
    inproc_transport<> ta{ex, bus};
    inproc_transport<> tb{ex, bus};
    static_discovery   disc{{}};

    plexus::node_id id_a{make_id(0x0A)};
    plexus::node_id id_b{make_id(0x0B)};

    // A dials eagerly (single-dialer: exactly one connection); B accepts.
    inproc_node a{ex, disc, id_a, ta, make_opts(/*eager=*/true)};
    inproc_node b{ex, disc, id_b, tb, make_opts(/*eager=*/false)};

    void pump() { ex.drain(); }

    void connect()
    {
        a.listen({"inproc", "host-a:5000"});
        b.listen({"inproc", "host-b:6000"});
        pump();
        REQUIRE(a.router().is_connected(id_b));
    }
};

}

// Family 1: no thread is spawned by node construction or by a full pub/sub + call
// round-trip — the OS thread count is identical before and after (Linux). The host owns
// the only loop, and it is driven from the test thread.
TEST_CASE("borrowed executor: no thread is spawned across construction and a full round-trip",
          "[node][borrowed-executor]")
{
#ifdef __linux__
    const std::size_t before = os_thread_count();
#endif

    host_bridge h;
    h.connect();

    std::vector<std::string> got;
    inproc_subscriber sub{h.a, "topic",
                          [&](std::span<const std::byte> b) { got.push_back(to_string(b)); }};
    inproc_publisher  pub{h.b, "topic"};

    inproc_procedure proc{
            h.b, "rpc", [](std::span<const std::byte> param, inproc_procedure::reply_fn &reply)
            { reply(plexus::wire::rpc_status::success, as_bytes("reply:" + to_string(param))); }};
    inproc_caller call{h.a, "rpc"};
    h.pump();

    pub.publish(as_bytes("payload"));
    h.pump();
    REQUIRE(got.size() == 1);

    std::optional<std::string> rpc_got;
    call.call(as_bytes("ping"),
              [&](plexus::expected<reply_t, std::error_code> r)
              {
                  REQUIRE(static_cast<bool>(r));
                  rpc_got = to_string(r.value().bytes);
              });
    h.pump();
    REQUIRE(rpc_got == "reply:ping");

#ifdef __linux__
    const std::size_t after = os_thread_count();
    REQUIRE(after == before);
#endif
}

// Family 2: nothing progresses unless the host pumps. The no-progress assertion is
// sampled at several pipeline stages — after construction, after the subscribe demand,
// and after the publish — and each time the work resolves ONLY once the host steps the
// loop. The node never drives its own loop.
TEST_CASE("borrowed executor: no delivery happens until the host pumps the loop",
          "[node][borrowed-executor]")
{
    host_bridge h;
    h.connect();

    std::vector<std::string> got;

    // Sample point 1: a fresh subscriber's standing fan is posted, not run inline.
    inproc_subscriber sub{h.a, "topic",
                          [&](std::span<const std::byte> b) { got.push_back(to_string(b)); }};
    inproc_publisher  pub{h.b, "topic"};
    REQUIRE(got.empty());

    // Sample point 2: still nothing delivered before the loop turns; the subscribe demand
    // only reaches the publisher once the host pumps.
    REQUIRE(got.empty());
    h.pump();
    REQUIRE(got.empty());

    // Sample point 3: a publish with NO pump delivers nothing; only the host pump moves it.
    pub.publish(as_bytes("held"));
    REQUIRE(got.empty());

    REQUIRE(pump_until(h.ex, [&] { return !got.empty(); }));
    REQUIRE(got.size() == 1);
    REQUIRE(got.front() == "held");
}

// The round-trip is itself looped under the host pump: every iteration the host drives
// the loop and the call completes, proving the no-own-loop property holds repeatedly, not
// once.
TEST_CASE("borrowed executor: the host-pumped round-trip is reproducible, looped",
          "[node][borrowed-executor]")
{
    constexpr int k_iterations = 8;
    host_bridge   h;
    h.connect();

    inproc_procedure proc{
            h.b, "rpc", [](std::span<const std::byte> param, inproc_procedure::reply_fn &reply)
            { reply(plexus::wire::rpc_status::success, as_bytes("reply:" + to_string(param))); }};
    inproc_caller call{h.a, "rpc"};
    h.pump();

    int proven = 0;
    for(int i = 0; i < k_iterations; ++i)
    {
        const std::string          req = "req-" + std::to_string(i);
        std::optional<std::string> got;
        call.call(as_bytes(req),
                  [&](plexus::expected<reply_t, std::error_code> r)
                  {
                      REQUIRE(static_cast<bool>(r));
                      got = to_string(r.value().bytes);
                  });
        // Before the host pumps this iteration's call, nothing has completed.
        REQUIRE_FALSE(got.has_value());
        h.pump();
        REQUIRE(got == "reply:" + req);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}
