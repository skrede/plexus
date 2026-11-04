#include "test_session_malformed_common.h"

#include "plexus/io/detail/drop_event.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/peer_report.h"
#include "plexus/wire/forwarded_frame.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>

using namespace session_malformed_fixture;
using plexus::io::detail::drop_cause;

namespace {

constexpr std::uint32_t k_flood = 1000;

// The origin is a third node and the hop is inside the default budget, so the acceptor admits it.
std::vector<std::byte> admitted_envelope(std::vector<std::byte> inner)
{
    const wire::forwarded_frame ff{.origin = make_cfg(0x03).self_id, .destination = {}, .hop = 1, .seq = 0, .flags = 0, .inner = std::move(inner)};
    return frame_of(wire::msg_type::forwarded, wire::encode_forwarded_frame(ff));
}

void install_refan(established_acceptor &f)
{
    f.messages.on_forward_refan([](auto &&...) {});
}

void check_counted_once(const established_acceptor &f)
{
    CHECK(f.fx.drops == std::vector{drop_cause::malformed});
    CHECK(f.subject->is_complete());
}

}

TEST_CASE("session malformed: a truncated peer report is counted once", "[integration][session_malformed]")
{
    established_acceptor f;
    wire::peer_report pr{};
    pr.origin                     = make_cfg(0x03).self_id;
    std::vector<std::byte> report = wire::encode_peer_report(pr);
    report.pop_back();

    f.inject(frame_of(wire::msg_type::peer_report, report));

    check_counted_once(f);
}

TEST_CASE("session malformed: a forwarded envelope whose inner frame has no header is counted once", "[integration][session_malformed][!shouldfail]")
{
    established_acceptor f;

    f.inject(admitted_envelope({std::byte{0}}));

    check_counted_once(f);
}

TEST_CASE("session malformed: a forwarded data body consumed locally is counted once", "[integration][session_malformed][!shouldfail]")
{
    established_acceptor f;

    f.inject(admitted_envelope(frame_of(wire::msg_type::unidirectional, {})));

    check_counted_once(f);
}

TEST_CASE("session malformed: a forwarded data body a relay re-fans without consuming is counted once", "[integration][session_malformed][!shouldfail]")
{
    established_acceptor f;
    f.messages.set_consume_relayed(false);
    install_refan(f);

    f.inject(admitted_envelope(frame_of(wire::msg_type::unidirectional, {})));

    check_counted_once(f);
}

TEST_CASE("session malformed: a forwarded data body that is consumed and re-fanned is counted once, not twice", "[integration][session_malformed][!shouldfail]")
{
    established_acceptor f;
    install_refan(f);

    f.inject(admitted_envelope(frame_of(wire::msg_type::unidirectional, {})));

    check_counted_once(f);
}

TEST_CASE("session malformed: a forwarded rpc request with an undecodable body is counted once", "[integration][session_malformed][!shouldfail]")
{
    established_acceptor f;

    f.inject(admitted_envelope(frame_of(wire::msg_type::rpc_request, {})));

    check_counted_once(f);
}

TEST_CASE("session malformed: a forwarded rpc response with an undecodable body is counted once", "[integration][session_malformed][!shouldfail]")
{
    established_acceptor f;

    f.inject(admitted_envelope(frame_of(wire::msg_type::rpc_response, {})));

    check_counted_once(f);
}

TEST_CASE("session malformed: a plain data body on a re-fanning session is counted once, not twice", "[integration][session_malformed][!shouldfail]")
{
    established_acceptor f;
    install_refan(f);

    f.inject(frame_of(wire::msg_type::unidirectional, {}));

    check_counted_once(f);
}

TEST_CASE("session malformed: a flood of undecodable data or rpc payloads logs one line", "[integration][session_malformed][!shouldfail]")
{
    for(const wire::msg_type type : std::array{wire::msg_type::unidirectional, wire::msg_type::rpc_request, wire::msg_type::rpc_response})
    {
        established_acceptor f;
        INFO("msg_type " << static_cast<std::uint32_t>(type));
        const std::uint32_t before = f.sink.lines;

        for(std::uint32_t i = 0; i < k_flood; ++i)
            f.inject(frame_of(type, {}));

        CHECK(f.fx.drops.size() == k_flood);
        CHECK(f.sink.lines - before == 1);
    }
}

TEST_CASE("session malformed: an engine's observer and logger see an undecodable data frame", "[integration][session_malformed][!shouldfail]")
{
    engine_clock::reset();
    drop_recorder recorder;
    engine_with_raw_peer link;
    link.node.add_observer(recorder);
    REQUIRE(link.channel);
    link.channel->send(handshake_naming(0x02));
    link.ex.drain();

    link.channel->send(frame_of(wire::msg_type::unidirectional, {}));
    link.ex.drain();

    CHECK(recorder.seen == std::vector{drop_cause::malformed});
    CHECK(link.logger.malformed_lines().size() == 1);
}
