#include "test_session_gate_common.h"

#include "plexus/io/lifecycle_event.h"

#include "plexus/io/detail/drop_event.h"
#include "plexus/io/detail/forwarder_companion.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/subscribe.h"
#include "plexus/wire/topic_hash.h"

#include <catch2/catch_test_macros.hpp>

#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <algorithm>

using namespace session_gate_fixture;
using plexus::io::lifecycle_edge;
using plexus::io::detail::drop_cause;

namespace {

std::vector<std::byte> response_frame(std::uint8_t status_byte)
{
    const wire::subscribe_response resp{.topic_hash = wire::fqn_topic_hash(k_topic), .status = wire::subscribe_status::subscribed, .offer = wire::offer_state::offered};
    std::vector<std::byte> payload = wire::encode_subscribe_response(resp);
    payload[8]                     = std::byte{status_byte};
    return frame_of(wire::msg_type::subscribe_response, payload);
}

// The subscribe is sent before the handshake completes, so the completion's ready waits on that subscribe's response.
struct subscribing_acceptor : pre_handshake_acceptor
{
    subscribing_acceptor()
            : pre_handshake_acceptor()
            , degraded(0)
            , companion_events(0)
            , edges_before(0)
            , refused()
    {
        subject->subscribe(k_topic);
        record_outcomes();
        hear(handshake_request_frame());
        REQUIRE(subject->is_complete());
        edges_before = edges.size();
    }

    std::size_t ready_edges() const
    {
        return static_cast<std::size_t>(std::count(edges.begin() + static_cast<std::ptrdiff_t>(edges_before), edges.end(), lifecycle_edge::ready));
    }

    std::uint32_t degraded;
    std::uint32_t companion_events;
    std::size_t edges_before;
    std::vector<wire::subscribe_status> refused;

private:
    void record_outcomes()
    {
        subject->on_subscribe_refused([this](std::uint64_t, wire::subscribe_status status) { refused.push_back(status); });
        subject->on_subscribe_degraded([this](std::uint64_t, std::uint8_t) { ++degraded; });
        plexus::io::detail::companion_hooks<inproc_channel<>> hooks;
        hooks.event_cb = [this](auto &&...) { ++companion_events; };
        messages.on_companion(std::move(hooks));
    }
};

}

TEST_CASE("subscribe refusal: a type_undeclared response fires on_subscribe_refused and notes no offer", "[integration][subscribe_refusal]")
{
    subscribing_acceptor f;
    REQUIRE(f.ready_edges() == 0);

    f.inject(response_frame(static_cast<std::uint8_t>(wire::subscribe_status::type_undeclared)));

    CHECK(f.refused == std::vector{wire::subscribe_status::type_undeclared});
    CHECK(f.degraded == 0);
    CHECK(f.companion_events == 0);
    CHECK(f.ready_edges() == 1);
}

TEST_CASE("subscribe refusal: a response with an unknown status byte is counted malformed", "[integration][subscribe_refusal]")
{
    subscribing_acceptor f;

    f.inject(response_frame(0x0A));

    CHECK(f.fx.drops == std::vector{drop_cause::malformed});
    CHECK(f.refused.empty());
    CHECK(f.degraded == 0);
    CHECK(f.companion_events == 0);
    CHECK(f.ready_edges() == 0);
}
