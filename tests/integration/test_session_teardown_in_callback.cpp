#include "test_session_gate_common.h"

#include "plexus/io/lifecycle_event.h"

#include "plexus/io/detail/forwarder_companion.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/subscribe.h"
#include "plexus/wire/topic_hash.h"
#include "plexus/wire/forwarded_frame.h"

#include "plexus/node_id.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <algorithm>
#include <string_view>

using namespace session_gate_fixture;
using plexus::io::lifecycle_edge;

namespace {

std::vector<std::byte> admitted_envelope(std::vector<std::byte> inner)
{
    const wire::forwarded_frame ff{.origin = make_cfg(0x03).self_id, .destination = {}, .hop = 1, .seq = 0, .flags = 0, .inner = std::move(inner)};
    return frame_of(wire::msg_type::forwarded, wire::encode_forwarded_frame(ff));
}

std::vector<std::byte> degraded_response()
{
    const wire::subscribe_response resp{.topic_hash     = wire::fqn_topic_hash(k_topic),
                                        .status         = wire::subscribe_status::subscribed_degraded,
                                        .offer          = wire::offer_state::offered,
                                        .has_degraded   = true,
                                        .degraded_flags = 1};
    return frame_of(wire::msg_type::subscribe_response, wire::encode_subscribe_response(resp));
}

struct teardown_acceptor : established_acceptor
{
    teardown_acceptor()
            : established_acceptor()
            , refans(0)
            , declines(0)
            , edges_before(edges.size())
            , originations(0)
            , companion_events(0)
    {
    }

    bool saw(lifecycle_edge edge) const
    {
        return std::find(edges.begin() + static_cast<std::ptrdiff_t>(edges_before), edges.end(), edge) != edges.end();
    }

    void tear_down_on_message()
    {
        subject->on_message([this](std::string_view, std::span<const std::byte>) { subject->tear_down(); });
    }

    void check_torn_down() const
    {
        CHECK_FALSE(subject->is_complete());
        CHECK(saw(lifecycle_edge::disconnected));
    }

    std::uint32_t refans;
    std::uint32_t declines;
    std::size_t edges_before;
    std::uint32_t originations;
    std::uint32_t companion_events;
};

}

TEST_CASE("session teardown: a message callback that tears the session down stops the re-origination", "[integration][session_teardown]")
{
    teardown_acceptor f;
    f.messages.on_forward_refan([](auto &&...) {});
    f.messages.on_originate([&f](auto &&...) { ++f.originations; });
    f.tear_down_on_message();

    f.inject(data_frame("x"));

    CHECK(f.originations == 0);
    f.check_torn_down();
}

TEST_CASE("session teardown: a message callback that tears the session down stops the forwarded re-fan", "[integration][session_teardown]")
{
    teardown_acceptor f;
    f.messages.on_forward_refan([&f](auto &&...) { ++f.refans; });
    f.tear_down_on_message();

    f.inject(admitted_envelope(data_frame("x")));

    CHECK(f.refans == 0);
    f.check_torn_down();
}

TEST_CASE("session teardown: a stamp callback that tears the session down stops the decline callback", "[integration][session_teardown]")
{
    teardown_acceptor f;
    f.subject->on_stamp_seen([&f](const plexus::node_id &) { f.subject->tear_down(); });
    f.subject->on_decline_seen([&f](const plexus::node_id &, bool) { ++f.declines; });

    f.inject(frame_for(wire::msg_type::heartbeat));

    CHECK(f.declines == 0);
    f.check_torn_down();
}

TEST_CASE("session teardown: a degraded subscribe callback that tears the session down stops note_offer", "[integration][session_teardown]")
{
    teardown_acceptor f;
    plexus::io::detail::companion_hooks<inproc_channel<>> hooks;
    hooks.event_cb = [&f](auto &&...) { ++f.companion_events; };
    f.messages.on_companion(std::move(hooks));
    f.subject->subscribe(k_topic);
    f.settle();
    f.subject->on_subscribe_degraded([&f](std::uint64_t, std::uint8_t) { f.subject->tear_down(); });

    f.inject(degraded_response());

    CHECK(f.companion_events == 0);
    f.check_torn_down();
}
