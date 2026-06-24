#include "test_message_info_common.h"

using namespace message_info_fixture;

TEST_CASE("message_info: the existing 2-arg deliver hands up the topic and bytes", "[forwarder][message_info]")
{
    inproc_bus<>      bus;
    inproc_executor<> ex(bus);
    inproc_channel<>  ch(ex);
    auto              peer = make_peer(ch, "node-a");

    plexus::log::null_logger sink;
    forwarder                fwd{sink};
    REQUIRE(fwd.attach(peer, "alpha"));
    ex.drain();

    const std::string                   body = "payload";
    plexus::wire::unidirectional_header uhdr{.source = plexus::wire::endpoint_source_type::publisher, .sequence = 7, .topic_hash = plexus::wire::fqn_topic_hash("alpha")};
    auto                                inner = plexus::wire::encode_unidirectional(uhdr, as_bytes(body));

    std::string got_fqn;
    std::string got_bytes;
    fwd.deliver(peer, inner, plexus::node_id{}, /*has_source_identity=*/false,
                [&](std::string_view fqn, std::span<const std::byte> data)
                {
                    got_fqn.assign(fqn);
                    got_bytes.assign(reinterpret_cast<const char *>(data.data()), data.size());
                });

    CHECK(got_fqn == "alpha");
    CHECK(got_bytes == body);
}

TEST_CASE("message_info: the metadata overload delivers a fully-populated info", "[forwarder][message_info]")
{
    inproc_bus<>      bus;
    inproc_executor<> ex(bus);
    inproc_channel<>  ch(ex);
    auto              peer = make_peer(ch, "node-a");

    plexus::log::null_logger sink;
    forwarder                fwd{sink};
    REQUIRE(fwd.attach(peer, "alpha"));
    ex.drain();

    const std::string                   body       = "payload";
    constexpr std::uint64_t             k_sequence = 42;
    plexus::wire::unidirectional_header uhdr{.source = plexus::wire::endpoint_source_type::publisher, .sequence = k_sequence, .topic_hash = plexus::wire::fqn_topic_hash("alpha")};
    auto                                inner = plexus::wire::encode_unidirectional(uhdr, as_bytes(body));

    // The header-derived half of message_info as the session stamps it at on_receive:
    // a known source_timestamp from the frame header and a later receiver stamp.
    message_info session_info{};
    session_info.source_timestamp    = 1000;
    session_info.reception_timestamp = 2000;
    session_info.from_intra_process  = true;

    std::string  got_fqn;
    std::string  got_bytes;
    message_info got{};
    bool         delivered = false;
    fwd.deliver(peer, inner, session_info, peer_node_id(), /*has_source_identity=*/false,
                [&](std::string_view fqn, std::span<const std::byte> data, const message_info &mi)
                {
                    delivered = true;
                    got_fqn.assign(fqn);
                    got_bytes.assign(reinterpret_cast<const char *>(data.data()), data.size());
                    got = mi;
                });

    REQUIRE(delivered);
    CHECK(got_fqn == "alpha");
    CHECK(got_bytes == body);
    CHECK(got.publication_sequence == k_sequence);          // filled by the forwarder
    CHECK(got.source_timestamp == 1000);                    // == frame_header.timestamp_ns
    CHECK(got.reception_timestamp == 2000);                 // receiver-stamped
    CHECK(got.reception_timestamp >= got.source_timestamp); // monotonic
    CHECK(got.from_intra_process == true);
    CHECK_FALSE(got.source_identity.has_value()); // flag clear → source identity absent
}

TEST_CASE("message_info: the metadata overload reconstructs source_identity from a flag-gated gid", "[forwarder][message_info][gid]")
{
    inproc_bus<>      bus;
    inproc_executor<> ex(bus);
    inproc_channel<>  ch(ex);
    auto              peer = make_peer(ch, "node-a");

    plexus::log::null_logger sink;
    forwarder                fwd{sink};
    REQUIRE(fwd.attach(peer, "alpha"));
    ex.drain();

    const std::string                   body      = "payload";
    constexpr std::uint64_t             k_counter = 0x2A; // the endpoint counter on the wire
    plexus::wire::unidirectional_header uhdr{.source = plexus::wire::endpoint_source_type::publisher, .sequence = 9, .topic_hash = plexus::wire::fqn_topic_hash("alpha")};
    // A flag-gated frame: the varint endpoint counter rides the inner payload.
    auto inner = plexus::wire::encode_unidirectional(uhdr, as_bytes(body), k_counter);

    message_info got{};
    bool         delivered = false;
    fwd.deliver(peer, inner, message_info{}, peer_node_id(0xCD), /*has_source_identity=*/true,
                [&](std::string_view, std::span<const std::byte>, const message_info &mi)
                {
                    delivered = true;
                    got       = mi;
                });

    REQUIRE(delivered);
    REQUIRE(got.source_identity.has_value());
    // Reconstructed as session.peer_node_id ‖ counter — the node_id half is the PINNED
    // session peer (direct-delivery invariant), NOT taken from the frame.
    CHECK(got.source_identity->node_id() == peer_node_id(0xCD));
    CHECK(got.source_identity->endpoint_counter() == k_counter);
    CHECK(*got.source_identity == publisher_gid{peer_node_id(0xCD), k_counter});
}
