// Wave-0 scaffold for the subscribe-time type_id match (activated by a later plan).
//
// The producer-side reaction to an arriving subscribe (attach_for_fanout) today
// always replies subscribe_status::subscribed and ignores the subscribe_request's
// type_hash. The owning plan activates the dead type_hash field: a subscriber whose
// type_id matches the producer's stays subscribed; a mismatch is refused with
// subscribe_status::type_mismatch.
//
// This file is RED-NOW: the type_mismatch case is tagged [!shouldfail] because the
// compare does not exist yet (the producer replies `subscribed` regardless). When
// the owning plan lands the type_id compare, drop the [!shouldfail] tag and the case
// flips GREEN. The matching case is already correct and stays GREEN.

#include "plexus/io/message_forwarder.h"

#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_bus.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/subscribe.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/topic_hash.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <vector>
#include <cstddef>
#include <optional>

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_channel;
using plexus::inproc::inproc_executor;
using forwarder = plexus::io::message_forwarder<inproc_policy>;

namespace {

// A capture sink recording every frame the forwarder sends, so the test can decode
// the subscribe_response the producer emitted.
struct capture
{
    explicit capture(inproc_executor<> &ex) : sink(ex)
    {
        sink.on_data([this](std::span<const std::byte> d) {
            frames.emplace_back(d.begin(), d.end());
        });
    }

    inproc_channel<> sink;
    std::vector<std::vector<std::byte>> frames;
};

forwarder::peer make_peer(inproc_channel<> &fwd_channel, capture &cap, std::string node_name)
{
    fwd_channel.connect_to(cap.sink.local_endpoint());
    return forwarder::peer{fwd_channel, std::move(node_name)};
}

// Decode the FIRST subscribe_response status in a capture's recorded traffic.
std::optional<plexus::wire::subscribe_status> first_response_status(const capture &cap)
{
    for(const auto &f : cap.frames)
    {
        auto hdr = plexus::wire::decode_header(f);
        if(!hdr || hdr->type != plexus::wire::msg_type::subscribe_response)
            continue;
        auto inner = std::span<const std::byte>{f}.subspan(plexus::wire::header_size);
        if(auto resp = plexus::wire::decode_subscribe_response(inner))
            return resp->status;
    }
    return std::nullopt;
}

}

TEST_CASE("type_id_match: a matching type_id stays subscribed", "[forwarder][type_id]")
{
    inproc_bus<> bus;
    inproc_executor<> ex(bus);
    inproc_channel<> ch(ex);
    capture cap(ex);
    auto peer = make_peer(ch, cap, "node-a");

    forwarder fwd;
    REQUIRE(fwd.attach_for_fanout(peer, "alpha"));
    ex.drain();

    const auto status = first_response_status(cap);
    REQUIRE(status.has_value());
    CHECK(*status == plexus::wire::subscribe_status::subscribed);
}

// RED-NOW until the owning plan activates the type_id compare: the producer today
// replies `subscribed` and never inspects the subscriber's type_hash, so a mismatch
// is NOT yet refused. The [!shouldfail] tag pins the expected failure; remove it
// when the compare lands.
TEST_CASE("type_id_match: a mismatched type_id is refused with type_mismatch",
          "[forwarder][type_id][!shouldfail]")
{
    inproc_bus<> bus;
    inproc_executor<> ex(bus);
    inproc_channel<> ch(ex);
    capture cap(ex);
    auto peer = make_peer(ch, cap, "node-a");

    forwarder fwd;
    REQUIRE(fwd.attach_for_fanout(peer, "alpha"));
    ex.drain();

    const auto status = first_response_status(cap);
    REQUIRE(status.has_value());
    CHECK(*status == plexus::wire::subscribe_status::type_mismatch);
}
