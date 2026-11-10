// Debug turns the single-executor check on, and under it a moved-from call aborts; these cases pin the inert path.
#undef PLEXUS_AFFINITY_CHECKS

#include "plexus/recorder.h"
#include "plexus/procedure.h"
#include "plexus/publisher.h"
#include "plexus/subscriber.h"
#include "plexus/graph_change_handle.h"

#include "plexus/io/recording/byte_sink.h"

#include "plexus/wire/rpc_status.h"

#include "test_self_loopback_common.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>
#include <stdexcept>

using plexus_test::fixture;

namespace {

using bytes_procedure = plexus::procedure<>;

constexpr std::array<std::byte, 3> k_payload{std::byte{1}, std::byte{2}, std::byte{3}};

class null_sink final : public plexus::io::recording::byte_sink
{
public:
    void write(std::span<const std::byte>) override
    {
    }
};

void reply_ok(std::span<const std::byte>, bytes_procedure::reply_fn &reply)
{
    reply(plexus::wire::rpc_status::success, {});
}

template<typename Use>
void with_moved_from_recorder(Use use)
{
    fixture f;
    null_sink sink;
    auto source = f.node().make_recorder(sink);
    auto target{std::move(source)};
    use(source);
    target.flush();
}

}

TEST_CASE("handles: a moved-from recorder's trigger is inert", "[node][handles]")
{
    with_moved_from_recorder([](auto &source) { source.trigger(); });
}

TEST_CASE("handles: a moved-from recorder's captured_span is 0", "[node][handles]")
{
    with_moved_from_recorder([](auto &source) { REQUIRE(source.captured_span() == 0); });
}

TEST_CASE("handles: a moved-from recorder's pump has no work", "[node][handles]")
{
    with_moved_from_recorder([](auto &source) { REQUIRE(!source.pump()); });
}

TEST_CASE("handles: a moved-from recorder's flush is inert", "[node][handles]")
{
    with_moved_from_recorder([](auto &source) { source.flush(); });
}

TEST_CASE("handles: a moved-from recorder's preamble is empty", "[node][handles]")
{
    with_moved_from_recorder([](auto &source) { REQUIRE(source.preamble().empty()); });
}

TEST_CASE("handles: moved-from subscriber, procedure, graph_change_handle and recorder handles destroy inertly", "[node][handles]")
{
    fixture f;
    null_sink sink;
    std::int32_t got     = 0;
    std::int32_t changed = 0;
    std::optional<plexus::subscriber<>> sub_source{std::in_place, f.node(), "topic", [&](std::span<const std::byte>) { ++got; }};
    std::optional<bytes_procedure> proc_source{std::in_place, f.node(), "rpc", &reply_ok};
    std::optional<plexus::graph_change_handle> graph_source{f.node().on_graph_change([&](std::uint64_t) { ++changed; })};
    std::optional<decltype(f.node().make_recorder(sink))> rec_source{f.node().make_recorder(sink)};
    plexus::subscriber<> sub{std::move(*sub_source)};
    bytes_procedure proc{std::move(*proc_source)};
    plexus::graph_change_handle graph{std::move(*graph_source)};
    auto rec{std::move(*rec_source)};
    sub_source.reset();
    proc_source.reset();
    graph_source.reset();
    rec_source.reset();
    plexus::publisher<> pub{f.node(), "topic"};
    f.drive();
    pub.publish(k_payload);
    f.drive();
    rec.flush();
    REQUIRE(got == 1);
    REQUIRE(changed >= 1);
    REQUIRE_THROWS_AS((bytes_procedure{f.node(), "rpc", &reply_ok}), std::runtime_error);
}
