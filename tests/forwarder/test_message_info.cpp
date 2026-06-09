// Wave-0 scaffold for the message_info subscriber-callback delivery (activated by a
// later plan).
//
// Today the forwarder's deliver path hands the subscriber only (fqn, bytes). The
// owning plan adds the opt-in callback overload that ALSO receives a message_info
// carrying {source_identity, publication_sequence, source_timestamp,
// reception_timestamp, from_intra_process}, and derives from_intra_process honestly
// from the delivering channel's locality tier (true on a same-process "inproc"
// channel, false on a remote "tcp" one).
//
// This file is RED-NOW: the message_info overload does not exist yet, so the
// metadata-delivery assertions are tagged [!shouldfail]. The existing 2-arg deliver
// is exercised to prove the substrate is wired; when the owning plan lands the
// overload, replace the [!shouldfail] cases with real message_info assertions.

#include "plexus/io/message_forwarder.h"
#include "plexus/io/message_info.h"

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
#include <optional>

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

// RED-NOW until the owning plan adds the message_info overload. The metadata-bearing
// deliver does not exist yet, so this asserts the not-yet-built contract via a flag
// that stays false today. Remove [!shouldfail] and assert a real message_info when
// the overload lands.
TEST_CASE("message_info: the metadata overload delivers a source-identity-bearing info",
          "[forwarder][message_info][!shouldfail]")
{
    bool delivered_message_info = false;

    // The 3-arg (fqn, bytes, const message_info&) deliver overload is the seam the
    // owning plan adds; until then no metadata reaches a subscriber.
    CHECK(delivered_message_info);
}

// RED-NOW: from_intra_process must be derived honestly from the delivering channel's
// locality tier — true on a same-process inproc channel, false on a remote tcp one.
// The derivation does not exist yet.
TEST_CASE("message_info: from_intra_process tracks the channel locality tier",
          "[forwarder][message_info][!shouldfail]")
{
    message_info inproc_delivery{};
    message_info tcp_delivery{};

    // The owning plan stamps from_intra_process from the channel scheme. Until then
    // both default to false, so the inproc expectation is unmet.
    CHECK(inproc_delivery.from_intra_process == true);
    CHECK(tcp_delivery.from_intra_process == false);
}
