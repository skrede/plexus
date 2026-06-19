// The caller error-code paths over the node facade. It proves:
//   - a call with NO connected provider completes (POSTED, not inline) with
//     call_errc::no_provider: the completion does NOT fire before the executor is pumped,
//     fires after a single pump, and never hangs — looped;
//   - the same completion signature carries a timeout (a provider that never replies)
//     resolved through the per-call deadline path, mapping to call_errc::timeout.

#include "plexus/node.h"
#include "plexus/caller.h"
#include "plexus/procedure.h"
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
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <system_error>

namespace {

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_transport;
using plexus::discovery::static_discovery;

using inproc_node      = plexus::node<inproc_policy, inproc_transport<>>;
using inproc_caller    = plexus::caller<>;
using inproc_procedure = plexus::procedure<>;
using reply_t          = plexus::reply;

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
    opts.redial_seed  = 0xE12Cu;
    opts.dial_eagerly = eager;
    return opts;
}

std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

// Step the executor until `done` holds or a wall-clock budget elapses. The inproc timer
// fires against the real steady_clock, so a pending deadline only expires as wall time
// passes — drain() returns at quiescence with the timer still pending. This bounded spin
// re-enters step() (which re-checks the timer each call) so the deadline resolves without
// an unbounded sleep; the budget caps it so a never-firing path is a bounded failure, not
// a hang.
template<typename Pred>
bool pump_until(inproc_executor<> &ex, Pred done, std::chrono::milliseconds budget)
{
    const auto deadline = std::chrono::steady_clock::now() + budget;
    while(!done())
    {
        ex.step();
        if(std::chrono::steady_clock::now() > deadline)
            return done();
    }
    return true;
}

}

TEST_CASE("caller errc: no-provider completes POSTED with no_provider and never hangs, looped",
          "[node][call][errc]")
{
    constexpr int k_iterations = 5;
    int           proven       = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        inproc_bus<>       bus;
        inproc_executor<>  ex{bus};
        inproc_transport<> ta{ex, bus};
        static_discovery   disc{{}};

        // A lone node with NO peer known — no connected provider for any fqn.
        inproc_node   a{ex, disc, make_id(0x0A), ta, make_opts(/*eager=*/false)};
        inproc_caller call{a, "rpc"};

        bool                           fired = false;
        std::optional<std::error_code> err;
        call.call(as_bytes(std::string{"req"}),
                  [&](plexus::expected<reply_t, std::error_code> r)
                  {
                      fired = true;
                      REQUIRE_FALSE(static_cast<bool>(r));
                      err = r.error();
                  });

        // POSTED, not inline: the completion must NOT have fired before the pump.
        REQUIRE_FALSE(fired);

        ex.drain();

        REQUIRE(fired);
        REQUIRE(err.has_value());
        REQUIRE(*err == plexus::call_errc::no_provider);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("caller errc: a provider that never replies resolves timeout through the deadline path",
          "[node][call][errc]")
{
    inproc_bus<>       bus;
    inproc_executor<>  ex{bus};
    inproc_transport<> ta{ex, bus};
    inproc_transport<> tb{ex, bus};
    static_discovery   disc{{}};

    const auto  id_a = make_id(0x0A);
    const auto  id_b = make_id(0x0B);
    inproc_node a{ex, disc, id_a, ta, make_opts(/*eager=*/true)};
    inproc_node b{ex, disc, id_b, tb, make_opts(/*eager=*/false)};
    a.listen({"inproc", "host-a:5000"});
    b.listen({"inproc", "host-b:6000"});
    ex.drain();
    REQUIRE(a.router().is_connected(id_b));

    // A provider that DROPS every request: it never invokes reply, so only the per-call
    // deadline resolves the call (rpc_status::timeout -> call_errc::timeout).
    inproc_procedure proc{
            b, "rpc",
            [](std::span<const std::byte>, inproc_procedure::reply_fn &) { /* never replies */ }};

    inproc_caller                  call{a, "rpc"};
    std::optional<std::error_code> err;
    plexus::call_options           opts;
    opts.deadline = std::chrono::milliseconds(20);
    call.call(as_bytes(std::string{"req"}), opts,
              [&](plexus::expected<reply_t, std::error_code> r)
              {
                  REQUIRE_FALSE(static_cast<bool>(r));
                  err = r.error();
              });
    REQUIRE(pump_until(ex, [&] { return err.has_value(); }, std::chrono::milliseconds(2000)));
    REQUIRE(*err == plexus::call_errc::timeout);
}
