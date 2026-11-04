#include "test_session_malformed_common.h"

#include "plexus/io/detail/drop_event.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/frame_codec.h"

#include "support/alloc_counter.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <vector>
#include <cstddef>
#include <cstdint>

using namespace session_malformed_fixture;
using plexus::io::detail::drop_cause;

namespace {

constexpr std::uint32_t k_flood = 1000;

constexpr std::array k_decoding_consumers{
        wire::msg_type::handshake_req,   wire::msg_type::handshake_resp,  wire::msg_type::heartbeat,           wire::msg_type::subscribe_response,
        wire::msg_type::subscribe,       wire::msg_type::declare,         wire::msg_type::fetch_latched,       wire::msg_type::unsubscribe,
        wire::msg_type::companion_ready, wire::msg_type::companion_offer, wire::msg_type::companion_withdrawn, wire::msg_type::companion_switch,
        wire::msg_type::unidirectional,  wire::msg_type::rpc_request,     wire::msg_type::rpc_response,        wire::msg_type::peer_report,
        wire::msg_type::forwarded,
};
static_assert(k_decoding_consumers.size() == 17);

}

TEST_CASE("session malformed: an inner payload that fails to decode is counted", "[integration][session_malformed]")
{
    for(const wire::msg_type type : k_decoding_consumers)
    {
        established_acceptor f;
        REQUIRE(f.subject->is_complete());
        INFO("msg_type " << static_cast<std::uint32_t>(type));

        f.inject(frame_of(type, {}));

        CHECK(f.fx.drops == std::vector{drop_cause::malformed});
        CHECK(f.subject->is_complete());
    }
}

TEST_CASE("session malformed: a header that fails to decode is counted and closes the session", "[integration][session_malformed]")
{
    established_acceptor f;
    REQUIRE(f.subject->is_complete());

    f.inject(std::vector<std::byte>(wire::header_size, std::byte{0}));

    CHECK(f.fx.drops == std::vector{drop_cause::malformed});
    CHECK_FALSE(f.subject->is_complete());
}

TEST_CASE("session malformed: a flood inside one interval logs one line", "[integration][session_malformed]")
{
    established_acceptor f;
    const std::uint32_t before = f.sink.lines;

    for(std::uint32_t i = 0; i < k_flood; ++i)
        f.inject(frame_of(wire::msg_type::bidirectional, {}));

    CHECK(f.fx.drops.size() == k_flood);
    CHECK(f.sink.lines - before == 1);
}

TEST_CASE("session malformed: the malformed path does not allocate", "[integration][session_malformed]")
{
    established_acceptor f;
    const std::vector<std::byte> frame = undecodable_subscribe();
    f.fx.drops.reserve(k_flood + 1);
    f.inject(frame);

    plexus::testing::reset_alloc_count();
    const std::size_t before = plexus::testing::alloc_count();
    for(std::uint32_t i = 0; i < k_flood; ++i)
        f.inject(frame);
    const std::size_t after = plexus::testing::alloc_count();

    CHECK(f.fx.drops.size() == k_flood + 1);
    REQUIRE(after - before == 0);
}

TEST_CASE("session malformed: an engine's logger receives the warning", "[integration][session_malformed]")
{
    drop_recorder recorder;
    engine_with_raw_peer link;
    link.node.add_observer(recorder);
    REQUIRE(link.channel);
    link.channel->send(handshake_naming(0x02));
    link.ex.drain();
    const std::size_t before = link.logger.lines.size();

    link.channel->send(undecodable_subscribe());
    link.ex.drain();

    CHECK(recorder.seen == std::vector{drop_cause::malformed});
    CHECK(link.logger.lines.size() - before == 1);
}
