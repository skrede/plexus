#include "test_type_id_match_common.h"

using namespace type_id_match_fixture;

TEST_CASE("type_id_match: a matching type_id stays subscribed", "[forwarder][type_id]")
{
    inproc_bus<>      bus;
    inproc_executor<> ex(bus);
    inproc_channel<>  ch(ex);
    capture           cap(ex);
    auto              peer = make_peer(ch, cap, "node-a");

    plexus::log::null_logger sink;
    forwarder fwd{sink};
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
    inproc_bus<>      bus;
    inproc_executor<> ex(bus);
    inproc_channel<>  ch(ex);
    capture           cap(ex);
    auto              peer = make_peer(ch, cap, "node-a");

    plexus::log::null_logger sink;
    forwarder fwd{sink};
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
    inproc_bus<>      bus;
    inproc_executor<> ex(bus);
    inproc_channel<>  ch(ex);
    capture           cap(ex);
    auto              peer = make_peer(ch, cap, "node-a");

    plexus::log::null_logger sink;
    forwarder fwd{sink};
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
    inproc_bus<>      bus;
    inproc_executor<> ex(bus);
    inproc_channel<>  ch(ex);
    capture           cap(ex);
    auto              peer = make_peer(ch, cap, "node-a");

    plexus::log::null_logger sink;
    forwarder fwd{sink};
    fwd.declare("alpha", plexus::topic_qos{}, std::uint64_t{0xABCD});
    // The subscriber declares no type_id (std::nullopt) — absence is not a mismatch.
    REQUIRE(fwd.attach_for_fanout(peer, "alpha", std::nullopt));
    ex.drain();

    const auto status = first_response_status(cap);
    REQUIRE(status.has_value());
    CHECK(*status == plexus::wire::subscribe_status::subscribed);
}
