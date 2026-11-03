#include "test_session_gate_common.h"
#include "test_routing_engine_inproc_common.h"

#include "plexus/io/observer.h"

#include "plexus/io/detail/drop_event.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/handshake.h"
#include "plexus/wire/frame_codec.h"

#include "support/alloc_counter.h"

#include <catch2/catch_test_macros.hpp>

#include <memory>
#include <vector>
#include <cstddef>
#include <cstdint>

using namespace session_gate_fixture;
using plexus::io::detail::drop_cause;
using engine_clock = routing_inproc_fixture::manual_clock;

namespace {

constexpr std::uint32_t k_flood = 1000;

std::vector<std::byte> undecodable_subscribe()
{
    return frame_of(wire::msg_type::subscribe, {});
}

std::vector<std::byte> handshake_naming(std::uint8_t id_seed)
{
    const wire::handshake_request req{.id                       = make_cfg(id_seed).self_id,
                                      .version_major            = 1,
                                      .version_minor            = 0,
                                      .compatible_version_major = 1,
                                      .compatible_version_minor = 0,
                                      .protocol_version         = wire::k_protocol_version,
                                      .fingerprint              = 0,
                                      .key_id                   = {},
                                      .own_nonce                = {},
                                      .cipher_offer             = 0,
                                      .chosen_cipher            = 0,
                                      .proof                    = {}};
    return frame_of(wire::msg_type::handshake_req, wire::encode_handshake_request(req));
}

struct drop_recorder final : plexus::io::observer
{
    void on_drop(const plexus::io::detail::drop_event &ev) override
    {
        seen.push_back(ev.cause);
    }

    std::vector<drop_cause> seen;
};

// An engine accepting a bare channel the test speaks for, so the frames the engine's session hears are the test's.
struct engine_with_raw_peer
{
    engine_with_raw_peer()
            : bus()
            , ex(bus)
            , transport(ex, bus)
            , raw(ex, bus)
            , logger()
            , node(transport, ex, routing_inproc_fixture::make_cfg(0xA1), routing_inproc_fixture::k_long_timeout, routing_inproc_fixture::forever_cfg(), routing_inproc_fixture::k_seed,
                   logger)
            , channel()
    {
        node.listen({"inproc", "node-a"});
        raw.on_dialed([this](std::unique_ptr<inproc_channel<engine_clock>> ch, const plexus::io::endpoint &) { channel = std::move(ch); });
        raw.dial({"inproc", "node-a"});
        ex.drain();
    }

    inproc_bus<engine_clock> bus;
    inproc_executor<engine_clock> ex;
    routing_inproc_fixture::transport_t transport;
    routing_inproc_fixture::transport_t raw;
    counting_logger logger;
    routing_inproc_fixture::engine node;
    std::unique_ptr<inproc_channel<engine_clock>> channel;
};

}

TEST_CASE("session malformed: an inner payload that fails to decode is counted", "[integration][session_malformed][!shouldfail]")
{
    established_acceptor f;
    REQUIRE(f.subject->is_complete());

    f.inject(undecodable_subscribe());

    CHECK(f.fx.drops == std::vector{drop_cause::malformed});
    CHECK(f.subject->is_complete());
}

TEST_CASE("session malformed: a header that fails to decode is counted and closes the session", "[integration][session_malformed][!shouldfail]")
{
    established_acceptor f;
    REQUIRE(f.subject->is_complete());

    f.inject(std::vector<std::byte>(wire::header_size, std::byte{0}));

    CHECK(f.fx.drops == std::vector{drop_cause::malformed});
    CHECK_FALSE(f.subject->is_complete());
}

TEST_CASE("session malformed: a flood inside one interval logs one line", "[integration][session_malformed][!shouldfail]")
{
    established_acceptor f;
    const std::uint32_t before = f.sink.lines;

    for(std::uint32_t i = 0; i < k_flood; ++i)
        f.inject(frame_of(wire::msg_type::bidirectional, {}));

    CHECK(f.fx.drops.size() == k_flood);
    CHECK(f.sink.lines - before == 1);
}

TEST_CASE("session malformed: the malformed path does not allocate", "[integration][session_malformed][!shouldfail]")
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

TEST_CASE("session malformed: an engine's logger receives the warning", "[integration][session_malformed][!shouldfail]")
{
    drop_recorder recorder;
    engine_with_raw_peer link;
    link.node.add_observer(recorder);
    REQUIRE(link.channel);
    link.channel->send(handshake_naming(0x02));
    link.ex.drain();
    const std::uint32_t before = link.logger.lines;

    link.channel->send(undecodable_subscribe());
    link.ex.drain();

    CHECK(recorder.seen == std::vector{drop_cause::malformed});
    CHECK(link.logger.lines - before == 1);
}
