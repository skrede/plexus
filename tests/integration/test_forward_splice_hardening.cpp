// Hardening proofs for the relay pub/sub splice, all over the one shared multi-leg relay + drop-tap
// fixture: (1) an envelope too large for a pooled slot drops-with-count (splice_oversize) and is NEVER
// encoded — the fixed-slot writer is an unchecked memcpy, so under ASan an unclamped encode would overrun
// the slot's heap allocation; the drop is size-selective (a fitting frame transits). (2) A topic demanded
// by BOTH a same-node self-route and a remote subscriber originates the envelope to the remote leg ONLY —
// the self-route (which would decode framed bytes as unidirectional) receives nothing. (3) Pool exhaustion
// is accounted per affected destination, through the same drop sink as oversize — not a single under-count.

#include "plexus/io/message_forwarder.h"
#include "plexus/io/detail/forward_splice.h"
#include "plexus/io/detail/drop_event.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"

#include "plexus/io/null_logger.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/topic_hash.h"

#include "plexus/node_id.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <vector>
#include <memory>
#include <cstddef>
#include <cstdint>

using namespace plexus;
namespace wire = plexus::wire;

namespace {

using forwarder = io::message_forwarder<inproc::inproc_policy>;
using fwd_splice = io::forward_splice<inproc::inproc_policy>;

node_id id_of(std::uint8_t base)
{
    node_id id{};
    for(std::size_t i = 0; i < id.size(); ++i)
        id[i] = static_cast<std::byte>(base + i);
    return id;
}

io::forward_ctx ctx_with(std::size_t slots, std::size_t slot_bytes)
{
    io::forward_options opts;
    opts.splice_pool_slots = slots;
    opts.splice_slot_bytes = slot_bytes;
    return io::make_forward_ctx(opts);
}

// A header-on unidirectional frame for a topic captured off a producer's wire — the exact shape a relay
// splices inside a forwarded envelope; body_size scales the encoded envelope past a slot when needed.
std::vector<std::byte> capture_inner(std::string_view fqn, std::size_t body_size)
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
    producer.publish(fqn, std::vector<std::byte>(body_size, std::byte{0xA5}));
    ex.drain();
    return framed;
}

// One downstream leg: a tx the relay fans onto and a rx sink capturing whatever crossed.
struct leg
{
    explicit leg(inproc::inproc_executor<> &ex)
            : tx(ex)
            , rx(ex)
    {
        tx.connect_to(rx.local_endpoint());
        rx.on_data([this](std::span<const std::byte> f) { got.emplace_back(f.begin(), f.end()); });
    }
    inproc::inproc_channel<> tx;
    inproc::inproc_channel<> rx;
    std::vector<std::vector<std::byte>> got;
};

struct relay_fixture
{
    inproc::inproc_bus<> bus;
    inproc::inproc_executor<> ex{bus};
    log::null_logger log_sink;
    forwarder fwd{log_sink};
    std::vector<io::detail::drop_cause> drops;

    relay_fixture()
    {
        fwd.on_drop([this](const io::detail::drop_event &e) { drops.push_back(e.cause); });
    }

    std::size_t drops_of(io::detail::drop_cause cause) const
    {
        std::size_t n = 0;
        for(auto c : drops)
            n += (c == cause);
        return n;
    }
};

}

TEST_CASE("forward_splice hardening: an envelope larger than a pooled slot drops-with-count and is never "
          "encoded; a fitting frame transits",
          "[integration][forward_splice][hardening]")
{
    relay_fixture fx;
    leg dest{fx.ex};
    fx.fwd.declare("relayed.topic", topic_qos{});
    REQUIRE(fx.fwd.attach_for_fanout(forwarder::peer{dest.tx, "leaf"}, "relayed.topic"));
    fx.ex.drain();
    dest.got.clear(); // discard the attach handshake frames

    fwd_splice sp{ctx_with(/*slots=*/8, /*slot_bytes=*/2048)};
    const auto hash = wire::fqn_topic_hash("relayed.topic");

    // An envelope whose encoded size (outer header + preamble + inner) exceeds the 2048-byte slot: an
    // unclamped encode would memcpy past the slot's heap allocation (ASan trip). It must drop, not send.
    const auto oversized = capture_inner("relayed.topic", 4096);
    REQUIRE(oversized.size() > 2048u);
    sp.refan(fx.fwd, hash, id_of(0xB0), /*hop=*/1, oversized, /*arrival=*/nullptr, nullptr);
    fx.ex.drain();

    REQUIRE(dest.got.empty());                                          // never put on the wire
    REQUIRE(fx.drops_of(io::detail::drop_cause::splice_oversize) == 1u); // counted once (one destination)
    REQUIRE(sp.exhaustion_drops() == 0u);                              // not a pool-exhaustion drop

    // A fitting frame on the SAME relay transits — the drop above was size-selective, not a dead leg.
    const auto fitting = capture_inner("relayed.topic", 32);
    sp.refan(fx.fwd, hash, id_of(0xB0), /*hop=*/2, fitting, /*arrival=*/nullptr, nullptr);
    fx.ex.drain();
    REQUIRE(dest.got.size() == 1u);
    REQUIRE(fx.drops_of(io::detail::drop_cause::splice_oversize) == 1u); // the fitting frame added no drop
}

TEST_CASE("forward_splice hardening: a co-registered self-route never receives the forwarded envelope; the "
          "remote subscriber does",
          "[integration][forward_splice][hardening]")
{
    relay_fixture fx;
    leg remote{fx.ex};
    leg self{fx.ex};
    fx.fwd.declare("relayed.topic", topic_qos{});
    REQUIRE(fx.fwd.attach_for_fanout(forwarder::peer{remote.tx, "leaf"}, "relayed.topic"));
    fx.fwd.attach_local("relayed.topic", self.tx, "self"); // a same-node self-route on the SAME topic
    fx.ex.drain();
    remote.got.clear();
    self.got.clear();

    fwd_splice sp{ctx_with(8, 2048)};
    const auto inner = capture_inner("relayed.topic", 16);
    sp.refan(fx.fwd, wire::fqn_topic_hash("relayed.topic"), id_of(0xB0), /*hop=*/1, inner, /*arrival=*/nullptr, nullptr);
    fx.ex.drain();

    REQUIRE(remote.got.size() == 1u); // the remote subscriber receives the forwarded frame
    REQUIRE(self.got.empty());        // the self-route receives NOTHING (no 0x0F onto the self-channel)
}

TEST_CASE("forward_splice hardening: pool exhaustion is counted per affected destination", "[integration][forward_splice][hardening]")
{
    relay_fixture fx;
    leg d0{fx.ex};
    leg d1{fx.ex};
    leg d2{fx.ex};
    fx.fwd.declare("relayed.topic", topic_qos{});
    int n = 0;
    for(leg *d : {&d0, &d1, &d2})
        REQUIRE(fx.fwd.attach_for_fanout(forwarder::peer{d->tx, "leaf" + std::to_string(n++)}, "relayed.topic"));
    fx.ex.drain();
    for(leg *d : {&d0, &d1, &d2})
        d->got.clear(); // discard the attach handshake frames

    // Zero free slots models the exhaustion condition deterministically: the single build finds no slot,
    // and every one of the three destinations must be accounted — not collapsed into a single drop.
    fwd_splice sp{ctx_with(/*slots=*/0, /*slot_bytes=*/2048)};
    const auto inner = capture_inner("relayed.topic", 16);
    sp.refan(fx.fwd, wire::fqn_topic_hash("relayed.topic"), id_of(0xB0), /*hop=*/1, inner, /*arrival=*/nullptr, nullptr);
    fx.ex.drain();

    REQUIRE(d0.got.empty());
    REQUIRE(d1.got.empty());
    REQUIRE(d2.got.empty());
    REQUIRE(fx.drops_of(io::detail::drop_cause::splice_exhausted) == 3u); // one per destination, honest accounting
}
