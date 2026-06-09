// The subscribe-time type_id match: a producer reacting to an arriving subscribe
// (attach_for_fanout) compares the subscriber's declared type_id against its own
// declared producer type_id. A match (or either side undeclared) stays subscribed;
// a real mismatch is refused with subscribe_status::type_mismatch and NO fan-out
// entry is registered. The type_id rides the already-on-wire subscribe_request
// type_hash field (0 = undeclared); matching authority is subscribe-time discovery.

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
#include <cstdint>
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
    fwd.declare("alpha", plexus::topic_qos{}, std::uint64_t{0xABCD});
    REQUIRE(fwd.attach_for_fanout(peer, "alpha", std::uint64_t{0xABCD}));
    ex.drain();

    const auto status = first_response_status(cap);
    REQUIRE(status.has_value());
    CHECK(*status == plexus::wire::subscribe_status::subscribed);
}

TEST_CASE("type_id_match: a mismatched type_id is refused with type_mismatch",
          "[forwarder][type_id]")
{
    inproc_bus<> bus;
    inproc_executor<> ex(bus);
    inproc_channel<> ch(ex);
    capture cap(ex);
    auto peer = make_peer(ch, cap, "node-a");

    forwarder fwd;
    fwd.declare("alpha", plexus::topic_qos{}, std::uint64_t{0xABCD});
    // The subscriber declares a different type_id; the producer refuses it and does
    // NOT register the fan-out entry (attach_for_fanout returns false).
    REQUIRE_FALSE(fwd.attach_for_fanout(peer, "alpha", std::uint64_t{0x1234}));
    ex.drain();

    const auto status = first_response_status(cap);
    REQUIRE(status.has_value());
    CHECK(*status == plexus::wire::subscribe_status::type_mismatch);
}

TEST_CASE("type_id_match: an undeclared producer type accepts any subscriber type",
          "[forwarder][type_id]")
{
    inproc_bus<> bus;
    inproc_executor<> ex(bus);
    inproc_channel<> ch(ex);
    capture cap(ex);
    auto peer = make_peer(ch, cap, "node-a");

    forwarder fwd;
    // No declared producer type_id for "alpha": any subscriber type_id is accepted.
    REQUIRE(fwd.attach_for_fanout(peer, "alpha", std::uint64_t{0x1234}));
    ex.drain();

    const auto status = first_response_status(cap);
    REQUIRE(status.has_value());
    CHECK(*status == plexus::wire::subscribe_status::subscribed);
}

TEST_CASE("type_id_match: an undeclared subscriber type is accepted against a typed producer",
          "[forwarder][type_id]")
{
    inproc_bus<> bus;
    inproc_executor<> ex(bus);
    inproc_channel<> ch(ex);
    capture cap(ex);
    auto peer = make_peer(ch, cap, "node-a");

    forwarder fwd;
    fwd.declare("alpha", plexus::topic_qos{}, std::uint64_t{0xABCD});
    // The subscriber declares no type_id (std::nullopt) — absence is not a mismatch.
    REQUIRE(fwd.attach_for_fanout(peer, "alpha", std::nullopt));
    ex.drain();

    const auto status = first_response_status(cap);
    REQUIRE(status.has_value());
    CHECK(*status == plexus::wire::subscribe_status::subscribed);
}
