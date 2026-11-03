#include "test_session_gate_common.h"

#include "plexus/io/security_event.h"
#include "plexus/io/lifecycle_event.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/handshake.h"
#include "plexus/wire/frame_codec.h"

#include "plexus/node_id.h"

#include <catch2/catch_test_macros.hpp>

#include <vector>
#include <cstddef>
#include <cstdint>
#include <algorithm>

using namespace session_gate_fixture;
using plexus::io::lifecycle_edge;
using plexus::io::security_event;

namespace {

std::vector<std::byte> request_naming(std::uint8_t id_seed)
{
    const wire::handshake_request req{.id                       = make_cfg(id_seed).self_id,
                                      .version_major            = 1,
                                      .version_minor            = 0,
                                      .compatible_version_major = 1,
                                      .compatible_version_minor = 0,
                                      .protocol_version         = wire::k_protocol_version,
                                      .fingerprint              = 0,
                                      .key_id                   = {},
                                      .own_nonce                = {},
                                      .cipher_offer             = 0,
                                      .chosen_cipher            = 0,
                                      .proof                    = {}};
    return frame_of(wire::msg_type::handshake_req, wire::encode_handshake_request(req));
}

struct watched_acceptor : established_acceptor
{
    watched_acceptor()
            : established_acceptor()
            , edges_before(edges.size())
            , events()
    {
        subject->on_security([this](const security_event &e) { events.push_back(e); });
    }

    bool saw(lifecycle_edge edge) const
    {
        return std::find(edges.begin() + static_cast<std::ptrdiff_t>(edges_before), edges.end(), edge) != edges.end();
    }

    std::size_t edges_before;
    std::vector<security_event> events;
};

}

TEST_CASE("session identity: a later handshake naming another peer closes the session and keeps the proven id", "[integration][session_identity]")
{
    watched_acceptor f;
    const plexus::node_id proven = make_cfg(0x02).self_id;
    REQUIRE(f.subject->is_complete());
    REQUIRE(f.subject->peer_identity() == proven);
    f.hear(request_naming(0x03));
    REQUIRE(f.events.size() == 1);
    CHECK(f.events.front().kind == plexus::io::security_kind::identity_changed);
    CHECK(f.saw(lifecycle_edge::disconnected));
    CHECK_FALSE(f.saw(lifecycle_edge::rejected));
    CHECK_FALSE(f.subject->is_complete());
    CHECK(f.subject->peer_identity() == proven);
}

TEST_CASE("session identity: a later handshake from the proven peer is answered and the session stays complete", "[integration][session_identity]")
{
    watched_acceptor f;
    f.last_peer_frame.clear();
    f.hear(handshake_request_frame());
    const auto hdr = wire::decode_header(f.last_peer_frame);
    REQUIRE(hdr.has_value());
    CHECK(hdr->type == wire::msg_type::handshake_resp);
    CHECK(f.events.empty());
    CHECK(f.subject->is_complete());
    CHECK(f.subject->peer_identity() == make_cfg(0x02).self_id);
}
