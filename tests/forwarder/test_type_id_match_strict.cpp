#include "test_type_id_match_common.h"

using namespace type_id_match_fixture;

TEST_CASE("strict posture: a typed strict subscriber is refused by an undeclared producer",
          "[forwarder][type_id][strict]")
{
    inproc_bus<>      bus;
    inproc_executor<> ex(bus);
    inproc_channel<>  ch(ex);
    capture           cap(ex);
    auto              peer = make_peer(ch, cap, "node-a");

    forwarder fwd{};
    // No declared producer type for "alpha". A strict typed subscriber refuses to bind.
    REQUIRE_FALSE(fwd.attach_for_fanout(peer, "alpha", std::uint64_t{0xABCD}, strict_typed()));
    ex.drain();

    const auto status = first_response_status(cap);
    REQUIRE(status.has_value());
    CHECK(*status == plexus::wire::subscribe_status::type_undeclared);

    // No fan-out entry was registered: a later publish delivers nothing.
    cap.frames.clear();
    fwd.publish("alpha", as_bytes(std::string{"after-refusal"}));
    ex.drain();
    CHECK(count_data_frames(cap) == 0);
}

TEST_CASE("strict posture: a typed strict subscriber binds to a matching typed producer",
          "[forwarder][type_id][strict]")
{
    inproc_bus<>      bus;
    inproc_executor<> ex(bus);
    inproc_channel<>  ch(ex);
    capture           cap(ex);
    auto              peer = make_peer(ch, cap, "node-a");

    forwarder fwd{};
    fwd.declare("alpha", plexus::topic_qos{}, std::uint64_t{0xABCD});
    REQUIRE(fwd.attach_for_fanout(peer, "alpha", std::uint64_t{0xABCD}, strict_typed()));
    ex.drain();

    const auto status = first_response_status(cap);
    REQUIRE(status.has_value());
    CHECK(*status == plexus::wire::subscribe_status::subscribed);

    cap.frames.clear();
    fwd.publish("alpha", as_bytes(std::string{"delivered"}));
    ex.drain();
    CHECK(count_data_frames(cap) == 1);
}

TEST_CASE("strict posture: a lenient subscriber still binds to an undeclared producer",
          "[forwarder][type_id][strict]")
{
    inproc_bus<>      bus;
    inproc_executor<> ex(bus);
    inproc_channel<>  ch(ex);
    capture           cap(ex);
    auto              peer = make_peer(ch, cap, "node-a");

    forwarder fwd{};
    // A lenient (default-posture) typed subscriber attaches to an untyped producer —
    // the existing accepting-undeclared semantics are unchanged.
    REQUIRE(fwd.attach_for_fanout(peer, "alpha", std::uint64_t{0xABCD}));
    ex.drain();

    const auto status = first_response_status(cap);
    REQUIRE(status.has_value());
    CHECK(*status == plexus::wire::subscribe_status::subscribed);
}

TEST_CASE("strict posture: the typed-strict bit round-trips through the wire region",
          "[forwarder][type_id][strict]")
{
    const auto region = plexus::io::to_wire_region(strict_typed());
    const auto lifted = plexus::io::from_wire_region(region);
    CHECK(lifted.posture == plexus::io::attach_posture::strict);

    const auto lenient_region = plexus::io::to_wire_region(plexus::io::subscriber_qos{});
    CHECK((lenient_region.requested_flags & plexus::wire::detail::k_qos_flag_typed_strict) == 0);
    CHECK(plexus::io::from_wire_region(lenient_region).posture ==
          plexus::io::attach_posture::lenient);
}
