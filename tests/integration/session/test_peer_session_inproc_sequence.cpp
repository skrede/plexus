#include "test_peer_session_inproc_common.h"

#include "plexus/io/detail/drop_event.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/companion.h"
#include "plexus/wire/data_frame.h"
#include "plexus/wire/topic_hash.h"
#include "plexus/wire/frame_codec.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>

using namespace peer_session_inproc_fixture;
using plexus::io::detail::drop_cause;
using plexus::io::detail::drop_event;

namespace {

std::vector<std::byte> data_frame(std::uint64_t sequence, const std::string &payload)
{
    std::vector<std::byte> frame;
    const plexus::wire::frame_header hdr{.type = plexus::wire::msg_type::unidirectional, .flags = 0, .session_id = 0, .timestamp_ns = 0, .payload_len = 0};
    const plexus::wire::unidirectional_header uhdr{.source = plexus::wire::endpoint_source_type::publisher, .sequence = sequence, .topic_hash = plexus::wire::fqn_topic_hash("topic")};
    plexus::wire::encode_unidirectional_frame_into(frame, hdr, uhdr, as_bytes(payload));
    return frame;
}

std::vector<std::byte> switch_frame(std::uint64_t first_sequence)
{
    const auto marker = plexus::wire::encode_companion_switch({plexus::wire::fqn_topic_hash("topic"), first_sequence});
    const plexus::wire::frame_header hdr{.type = plexus::wire::msg_type::companion_switch, .flags = 0, .session_id = 0, .timestamp_ns = 0, .payload_len = marker.size()};
    return plexus::wire::encode_frame(hdr, marker);
}

struct demanded_link
{
    session_link l;
    std::uint32_t replays;

    demanded_link()
            : replays{0}
    {
        l.drive();
        REQUIRE(l.resp_messages.attach(l.responder->msg_peer(), "topic"));
        l.drive();
        l.responder->on_frame_dropped(
                [this](const drop_event &e)
                {
                    if(e.cause == drop_cause::replay)
                        ++replays;
                });
    }

    void receive(const std::vector<std::byte> &frame)
    {
        l.responder->on_receive(frame);
        l.drive();
    }
};

}

TEST_CASE("inproc peer_session: after a companion switch a frame already delivered from that peer is dropped as a replay", "[integration][peer_session][inproc]")
{
    demanded_link d;
    d.receive(switch_frame(10));
    const auto frame = data_frame(10, "ten");
    d.receive(frame);
    d.receive(frame);
    REQUIRE(d.l.resp_received.size() == 1);
    REQUIRE(d.l.resp_received[0] == "ten");
    REQUIRE(d.replays == 1);
}

TEST_CASE("inproc peer_session: after a companion switch an unseen older frame is still delivered", "[integration][peer_session][inproc]")
{
    demanded_link d;
    d.receive(switch_frame(10));
    d.receive(data_frame(12, "twelve"));
    d.receive(data_frame(11, "eleven"));
    REQUIRE(d.l.resp_received.size() == 2);
    REQUIRE(d.l.resp_received[0] == "twelve");
    REQUIRE(d.l.resp_received[1] == "eleven");
    REQUIRE(d.replays == 0);
}

TEST_CASE("inproc peer_session: without a companion switch a repeated frame is delivered each time", "[integration][peer_session][inproc]")
{
    demanded_link d;
    const auto frame = data_frame(10, "ten");
    d.receive(frame);
    d.receive(frame);
    REQUIRE(d.l.resp_received.size() == 2);
    REQUIRE(d.replays == 0);
}
