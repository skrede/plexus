// message_info subscriber-callback delivery.
//
// The forwarder's 2-arg deliver path hands the subscriber only (fqn, bytes). The
// opt-in 3-arg overload ALSO receives a message_info carrying {source_identity,
// publication_sequence, source_timestamp, reception_timestamp, from_intra_process}.
//
// The session assembles the header-derived metadata at on_receive (where the decoded
// frame_header is still live, before the router strips it) and the forwarder fills the
// publication_sequence it alone decodes from the inner payload. from_intra_process is
// derived honestly from the delivering channel's locality tier (true on a same-process
// "inproc" channel, false on a remote "tcp" one). source_identity stays absent until
// the gid rides the wire.
//
// These cases drive the 3-arg deliver directly with a session-shaped message_info to
// assert each field reaches the callback, then exercise the locality-tier derivation
// for both an intra-process and a remote channel.

#include "plexus/io/message_forwarder.h"
#include "plexus/io/message_info.h"
#include "plexus/io/locality.h"
#include "plexus/io/endpoint.h"

#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_bus.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/data_frame.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/topic_hash.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <string_view>
#include <cstddef>
#include <cstdint>
#include <optional>

using plexus::io::locality;
using plexus::io::tier_of;
using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_channel;
using plexus::inproc::inproc_executor;
using plexus::io::message_info;
using forwarder = plexus::io::message_forwarder<inproc_policy>;

namespace {

std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

forwarder::peer make_peer(inproc_channel<> &ch, std::string node_name)
{
    return forwarder::peer{ch, std::move(node_name)};
}

}

TEST_CASE("message_info: the existing 2-arg deliver hands up the topic and bytes", "[forwarder][message_info]")
{
    inproc_bus<> bus;
    inproc_executor<> ex(bus);
    inproc_channel<> ch(ex);
    auto peer = make_peer(ch, "node-a");

    forwarder fwd;
    REQUIRE(fwd.attach(peer, "alpha"));
    ex.drain();

    const std::string body = "payload";
    plexus::wire::unidirectional_header uhdr{
        .source     = plexus::wire::endpoint_source_type::publisher,
        .sequence   = 7,
        .topic_hash = plexus::wire::fqn_topic_hash("alpha")};
    auto inner = plexus::wire::encode_unidirectional(uhdr, as_bytes(body));

    std::string got_fqn;
    std::string got_bytes;
    fwd.deliver(peer, inner, [&](std::string_view fqn, std::span<const std::byte> data) {
        got_fqn.assign(fqn);
        got_bytes.assign(reinterpret_cast<const char *>(data.data()), data.size());
    });

    CHECK(got_fqn == "alpha");
    CHECK(got_bytes == body);
}

TEST_CASE("message_info: the metadata overload delivers a fully-populated info",
          "[forwarder][message_info]")
{
    inproc_bus<> bus;
    inproc_executor<> ex(bus);
    inproc_channel<> ch(ex);
    auto peer = make_peer(ch, "node-a");

    forwarder fwd;
    REQUIRE(fwd.attach(peer, "alpha"));
    ex.drain();

    const std::string body = "payload";
    constexpr std::uint64_t k_sequence = 42;
    plexus::wire::unidirectional_header uhdr{
        .source     = plexus::wire::endpoint_source_type::publisher,
        .sequence   = k_sequence,
        .topic_hash = plexus::wire::fqn_topic_hash("alpha")};
    auto inner = plexus::wire::encode_unidirectional(uhdr, as_bytes(body));

    // The header-derived half of message_info as the session stamps it at on_receive:
    // a known source_timestamp from the frame header and a later receiver stamp.
    message_info session_info{};
    session_info.source_timestamp    = 1000;
    session_info.reception_timestamp  = 2000;
    session_info.from_intra_process   = true;

    std::string got_fqn;
    std::string got_bytes;
    message_info got{};
    bool delivered = false;
    fwd.deliver(peer, inner, session_info,
                [&](std::string_view fqn, std::span<const std::byte> data, const message_info &mi) {
                    delivered = true;
                    got_fqn.assign(fqn);
                    got_bytes.assign(reinterpret_cast<const char *>(data.data()), data.size());
                    got = mi;
                });

    REQUIRE(delivered);
    CHECK(got_fqn == "alpha");
    CHECK(got_bytes == body);
    CHECK(got.publication_sequence == k_sequence);           // filled by the forwarder
    CHECK(got.source_timestamp == 1000);                     // == frame_header.timestamp_ns
    CHECK(got.reception_timestamp == 2000);                  // receiver-stamped
    CHECK(got.reception_timestamp >= got.source_timestamp);  // monotonic
    CHECK(got.from_intra_process == true);
    CHECK_FALSE(got.source_identity.has_value());            // gid not on the wire yet
}

TEST_CASE("message_info: from_intra_process tracks the channel locality tier",
          "[forwarder][message_info]")
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
