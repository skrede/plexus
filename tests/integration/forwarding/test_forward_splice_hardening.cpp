// Hardening proofs for the relay pub/sub splice, all over the one shared multi-leg relay + drop-tap
// fixture: (1) an envelope too large for a pooled slot drops-with-count (splice_oversize) and is NEVER
// encoded — the fixed-slot writer is an unchecked memcpy, so under ASan an unclamped encode would overrun
// the slot's heap allocation; the drop is size-selective (a fitting frame transits). (2) A topic demanded
// by BOTH a same-node self-route and a remote subscriber originates the envelope to the remote leg ONLY —
// the self-route (which would decode framed bytes as unidirectional) receives nothing. (3) Pool exhaustion
// is accounted per affected destination, through the same drop sink as oversize — not a single under-count.

#include "test_forward_splice_hardening_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace forward_splice_hardening_fixture;

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

    REQUIRE(dest.got.empty());                                           // never put on the wire
    REQUIRE(fx.drops_of(io::detail::drop_cause::splice_oversize) == 1u); // counted once (one destination)
    REQUIRE(sp.exhaustion_drops() == 0u);                                // not a pool-exhaustion drop

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
