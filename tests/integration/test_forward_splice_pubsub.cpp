// The relay pub/sub splice: an admitted forwarded frame is re-fanned ONLY to the destination sessions
// that actually subscribe the topic, wrapped in a fresh forwarded envelope carrying the trailer origin,
// and NEVER back onto the arrival session. Driven directly at the relay's forwarder + splice over inproc
// legs (the fan machinery, not a two-transport node), so the interest scoping and arrival exclusion are
// observable on the wire the destination receives.

#include "plexus/io/message_forwarder.h"
#include "plexus/io/detail/forward_splice.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"

#include "plexus/io/null_logger.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/topic_hash.h"
#include "plexus/wire/forwarded_frame.h"
#include "plexus/wire/frame_reassembler.h"

#include "plexus/node_id.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <type_traits>

using namespace plexus;
namespace wire = plexus::wire;

namespace {

using forwarder = io::message_forwarder<inproc::inproc_policy>;
using splice    = io::forward_splice<inproc::inproc_policy>;

std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

node_id id_of(std::uint8_t base)
{
    node_id id{};
    for(std::size_t i = 0; i < id.size(); ++i)
        id[i] = static_cast<std::byte>(base + i);
    return id;
}

io::forward_ctx ctx_of(io::splice_ownership mode)
{
    io::forward_options opts;
    opts.ownership = mode;
    return io::make_forward_ctx(opts);
}

// A header-on unidirectional frame for a topic, captured off a producer's wire — the exact shape a relay
// splices inside a forwarded envelope.
std::vector<std::byte> capture_inner(std::string_view fqn, std::string_view body)
{
    inproc::inproc_bus<> bus;
    inproc::inproc_executor<> ex(bus);
    inproc::inproc_channel<> sub(ex);
    inproc::inproc_channel<> cap(ex);
    sub.connect_to(cap.local_endpoint());
    std::vector<std::byte> framed;
    cap.on_data([&](std::span<const std::byte> f) { framed.assign(f.begin(), f.end()); });

    log::null_logger sink;
    forwarder producer{sink};
    producer.declare(fqn, topic_qos{});
    producer.attach_for_fanout(forwarder::peer{sub, "producer"}, fqn);
    ex.drain();
    producer.publish(fqn, as_bytes(std::string{body}));
    ex.drain();
    return framed;
}

// A relay fixture: a forwarder plus two downstream inproc legs (a subscribing destination and a second
// channel that stands in for the arrival session), each with its own capture sink.
struct relay_fixture
{
    inproc::inproc_bus<> bus;
    inproc::inproc_executor<> ex{bus};
    inproc::inproc_channel<> dest{ex};
    inproc::inproc_channel<> dest_sink{ex};
    inproc::inproc_channel<> arrival{ex};
    inproc::inproc_channel<> arrival_sink{ex};
    std::vector<std::vector<std::byte>> dest_got;
    std::vector<std::vector<std::byte>> arrival_got;
    log::null_logger log_sink;
    forwarder fwd{log_sink};

    relay_fixture()
    {
        dest.connect_to(dest_sink.local_endpoint());
        arrival.connect_to(arrival_sink.local_endpoint());
        dest_sink.on_data([this](std::span<const std::byte> f) { dest_got.emplace_back(f.begin(), f.end()); });
        arrival_sink.on_data([this](std::span<const std::byte> f) { arrival_got.emplace_back(f.begin(), f.end()); });
    }
};

}

TEST_CASE("forward_splice_pubsub: an admitted frame re-fans to the subscriber with the origin, never the "
          "arrival session",
          "[integration][forward_splice][pubsub]")
{
    relay_fixture fx;
    fx.fwd.declare("relayed.topic", topic_qos{});
    REQUIRE(fx.fwd.attach_for_fanout(forwarder::peer{fx.dest, "leaf"}, "relayed.topic"));
    REQUIRE(fx.fwd.attach_for_fanout(forwarder::peer{fx.arrival, "up"}, "relayed.topic")); // also a subscriber
    fx.ex.drain();
    fx.dest_got.clear();
    fx.arrival_got.clear();

    const node_id origin = id_of(0xB0);
    const auto inner     = capture_inner("relayed.topic", "relayed-body");
    REQUIRE_FALSE(inner.empty());

    splice sp{ctx_of(io::splice_ownership::pooled_owned_copy)};
    sp.refan(fx.fwd, wire::fqn_topic_hash("relayed.topic"), origin, /*hop=*/1, inner, &fx.arrival, nullptr);
    fx.ex.drain();

    REQUIRE(fx.arrival_got.empty());   // never re-fanned back onto the arrival session (loop guard)
    REQUIRE(fx.dest_got.size() == 1u); // the subscribing destination received exactly one frame

    auto hdr = wire::decode_header(fx.dest_got[0]);
    REQUIRE(hdr.has_value());
    REQUIRE(hdr->type == wire::msg_type::forwarded);
    const auto payload = std::span<const std::byte>{fx.dest_got[0]}.subspan(wire::header_size);
    auto ff            = wire::decode_forwarded_frame(payload);
    REQUIRE(ff.has_value());
    REQUIRE(ff->origin == origin);                                            // the trailer origin survives the splice
    REQUIRE(ff->hop == 2);                                                    // hop incremented on re-wrap
    REQUIRE(std::equal(ff->inner.begin(), ff->inner.end(), inner.begin(), inner.end())); // the inner frame is carried verbatim
}

TEST_CASE("forward_splice_pubsub: an undemanded topic is not fanned (registry scoping)", "[integration][forward_splice][pubsub]")
{
    relay_fixture fx;
    fx.fwd.declare("relayed.topic", topic_qos{});
    REQUIRE(fx.fwd.attach_for_fanout(forwarder::peer{fx.dest, "leaf"}, "relayed.topic"));
    fx.ex.drain();
    fx.dest_got.clear();

    const auto inner = capture_inner("relayed.topic", "body");
    splice sp{ctx_of(io::splice_ownership::pooled_owned_copy)};

    // A different topic nobody on this relay subscribes: the interest lookup finds no destination.
    sp.refan(fx.fwd, wire::fqn_topic_hash("other.topic"), id_of(0x10), 1, inner, nullptr, nullptr);
    fx.ex.drain();
    REQUIRE(fx.dest_got.empty());              // undemanded: nothing fanned
    REQUIRE(sp.exhaustion_drops() == 0u);      // and no pool slot was even taken (built lazily on the first destination)
}

TEST_CASE("forward_splice_pubsub: the refcounted zero-copy knob retains an inbound owner, never an empty "
          "one",
          "[integration][forward_splice][pubsub][zero-copy]")
{
    relay_fixture fx;
    fx.fwd.declare("relayed.topic", topic_qos{});
    REQUIRE(fx.fwd.attach_for_fanout(forwarder::peer{fx.dest, "leaf"}, "relayed.topic"));
    fx.ex.drain();
    fx.dest_got.clear();

    // A synthesized inbound owner (the owner-carrying receive seam the host relay retains): a complete
    // forwarded frame the zero-copy checkout shares to the destination with no slot copy.
    wire::forwarded_frame carried;
    carried.origin = id_of(0xC0);
    carried.hop    = 3;
    carried.seq    = 9;
    carried.inner  = capture_inner("relayed.topic", "zc-body");
    std::vector<std::byte> owned_bytes = wire::encode_frame(
            wire::frame_header{.type = wire::msg_type::forwarded, .flags = 0, .session_id = 0, .timestamp_ns = 0, .payload_len = 0}, wire::encode_forwarded_frame(carried));
    wire::shared_bytes owner{owned_bytes};

    splice sp{ctx_of(io::splice_ownership::refcounted_zero_copy)};
    sp.refan(fx.fwd, wire::fqn_topic_hash("relayed.topic"), carried.origin, carried.hop, carried.inner, nullptr, &owner);
    fx.ex.drain();

    REQUIRE(fx.dest_got.size() == 1u);                     // the retained owner was enqueued (never an empty owner)
    REQUIRE(fx.dest_got[0].size() == owned_bytes.size());  // the destination received the retained bytes, uncopied
}
