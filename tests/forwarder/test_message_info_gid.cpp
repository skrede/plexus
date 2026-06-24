#include "test_message_info_common.h"

using namespace message_info_fixture;

TEST_CASE("message_info: declare(emit_source_identity) mints a stable, distinct per-endpoint gid "
          "counter",
          "[forwarder][message_info][gid]")
{
    // Producer side: the endpoint counter is minted ONCE at the first source-identity
    // declare, is STABLE across a re-declare (so an endpoint's gid does not drift —
    // IDENT-02), and is DISTINCT per declared topic. Captured off the wire via a
    // subscribed channel so it exercises the real publish framing.
    inproc_bus<> bus;
    inproc_executor<> ex(bus);
    inproc_channel<> sub(ex);
    inproc_channel<> capture(ex);
    sub.connect_to(capture.local_endpoint());
    std::vector<std::byte> framed;
    capture.on_data([&](std::span<const std::byte> f) { framed.assign(f.begin(), f.end()); });
    auto peer = make_peer(sub, "node-rx");

    plexus::log::null_logger sink;
    forwarder fwd{sink};

    // The decoded endpoint counter of the LAST frame captured on the wire (nullopt when
    // the frame's gid flag was clear).
    auto published_counter = [&](std::string_view fqn) -> std::optional<std::uint64_t>
    {
        framed.clear();
        fwd.publish(fqn, as_bytes(std::string{"x"}));
        ex.drain();
        auto hdr = plexus::wire::decode_header(framed);
        REQUIRE(hdr.has_value());
        const bool flag = (hdr->flags & plexus::wire::k_flag_source_identity) != 0;
        auto inner      = std::span<const std::byte>(framed).subspan(plexus::wire::header_size);
        auto decoded    = plexus::wire::decode_unidirectional(inner, flag);
        REQUIRE(decoded.has_value());
        return decoded->endpoint_counter;
    };

    fwd.declare("alpha", plexus::topic_qos{}, std::nullopt, /*emit_source_identity=*/true);
    REQUIRE(fwd.attach_for_fanout(peer, "alpha"));
    ex.drain();
    const auto first = published_counter("alpha");
    REQUIRE(first.has_value());

    // Re-declare the SAME topic: the counter must NOT be re-minted (stable per endpoint).
    fwd.declare("alpha", plexus::topic_qos{}, std::nullopt, /*emit_source_identity=*/true);
    const auto after_redeclare = published_counter("alpha");
    REQUIRE(after_redeclare.has_value());
    CHECK(*after_redeclare == *first);

    // A SECOND source-identity topic mints a DISTINCT counter.
    fwd.declare("beta", plexus::topic_qos{}, std::nullopt, /*emit_source_identity=*/true);
    REQUIRE(fwd.attach_for_fanout(peer, "beta"));
    ex.drain();
    const auto beta = published_counter("beta");
    REQUIRE(beta.has_value());
    CHECK(*beta != *first);

    // A topic that did NOT declare source identity carries no counter (flag clear).
    fwd.declare("gamma", plexus::topic_qos{});
    REQUIRE(fwd.attach_for_fanout(peer, "gamma"));
    ex.drain();
    CHECK_FALSE(published_counter("gamma").has_value());
}

TEST_CASE("message_info: from_intra_process tracks the channel locality tier", "[forwarder][message_info]")
{
    // from_intra_process is derived from the delivering channel's OWN endpoint scheme,
    // never from peer-supplied data: an inproc channel is a genuine same-process
    // delivery; a tcp channel is remote.
    plexus::io::endpoint inproc_ep{"inproc", "node-a"};
    plexus::io::endpoint tcp_ep{"tcp", "127.0.0.1:9000"};

    const bool inproc_intra = tier_of(inproc_ep.scheme) == locality::process;
    const bool tcp_intra    = tier_of(tcp_ep.scheme) == locality::process;

    CHECK(inproc_intra == true);
    CHECK(tcp_intra == false);
}
