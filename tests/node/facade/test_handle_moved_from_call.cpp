// Debug turns the single-executor check on, and under it a moved-from call aborts; these cases pin the inert path.
#undef PLEXUS_AFFINITY_CHECKS

#include "plexus/caller.h"
#include "plexus/procedure.h"

#include "plexus/wire/rpc_status.h"

#include "test_call_family_common.h"
#include "test_self_loopback_common.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <system_error>

using plexus_test::sample;
using plexus_test::counting_codec;
using call_family_fixture::net;

namespace {

template<typename>
using sample_family = counting_codec;

using bytes_caller    = plexus::caller<>;
using typed_caller    = plexus::caller<sample(sample), sample_family>;
using bytes_procedure = plexus::procedure<>;
using typed_procedure = plexus::procedure<sample(sample), sample_family>;

constexpr std::array<std::byte, 3> k_param{std::byte{1}, std::byte{2}, std::byte{3}};

void serve(std::int32_t &served, bytes_procedure::reply_fn &reply)
{
    ++served;
    reply(plexus::wire::rpc_status::success, {});
}

plexus::expected<sample, std::error_code> echo(std::int32_t &served, const sample &request)
{
    ++served;
    return request;
}

template<typename Caller, typename Request>
void require_dropped(net &n, Caller &source, Caller &target, const Request &request, const std::int32_t &served)
{
    n.drive();
    std::int32_t completed = 0;
    source.call(request, [&](const auto &) { ++completed; });
    source.call(request, plexus::call_options{}, [&](const auto &) { ++completed; });
    n.drive();
    CHECK(served == 0);
    CHECK(completed == 0);
    target.call(request, [&](const auto &r) { completed += static_cast<bool>(r) ? 1 : 0; });
    n.drive();
    CHECK(served == 1);
    CHECK(completed == 1);
}

}

TEST_CASE("handles: a moved-from bytes caller's call dispatches nothing", "[node][handles]")
{
    net n;
    n.connect();
    std::int32_t served = 0;
    bytes_procedure proc{n.b, "rpc", [&](std::span<const std::byte>, bytes_procedure::reply_fn &reply) { serve(served, reply); }};
    bytes_caller source{n.a, "rpc"};
    bytes_caller target{std::move(source)};
    require_dropped(n, source, target, std::span<const std::byte>{k_param}, served);
}

TEST_CASE("handles: a bytes caller moved from by assignment dispatches nothing", "[node][handles]")
{
    net n;
    n.connect();
    std::int32_t served = 0;
    bytes_procedure proc{n.b, "rpc", [&](std::span<const std::byte>, bytes_procedure::reply_fn &reply) { serve(served, reply); }};
    bytes_caller source{n.a, "rpc"};
    bytes_caller target{n.a, "elsewhere"};
    target = std::move(source);
    require_dropped(n, source, target, std::span<const std::byte>{k_param}, served);
}

TEST_CASE("handles: a moved-from typed caller's call dispatches nothing", "[node][handles]")
{
    net n;
    n.connect();
    std::int32_t served = 0;
    typed_procedure proc{n.b, "rpc", [&](const sample &request) { return echo(served, request); }};
    typed_caller source{n.a, "rpc"};
    typed_caller target{std::move(source)};
    require_dropped(n, source, target, sample{3}, served);
}

TEST_CASE("handles: a typed caller moved from by assignment dispatches nothing", "[node][handles]")
{
    net n;
    n.connect();
    std::int32_t served = 0;
    typed_procedure proc{n.b, "rpc", [&](const sample &request) { return echo(served, request); }};
    typed_caller source{n.a, "rpc"};
    typed_caller target{n.a, "elsewhere"};
    target = std::move(source);
    require_dropped(n, source, target, sample{3}, served);
}
