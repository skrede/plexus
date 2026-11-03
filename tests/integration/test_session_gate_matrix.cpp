#include "test_session_gate_common.h"

#include "plexus/io/lifecycle_event.h"

#include "plexus/io/detail/drop_event.h"

#include "plexus/io/security/attach_facts.h"
#include "plexus/io/security/attach_policy.h"

#include "plexus/wire/frame.h"

#include "plexus/node_id.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <vector>
#include <cstdint>
#include <algorithm>

using namespace session_gate_fixture;
using plexus::io::lifecycle_edge;

namespace {

class refuse_all final : public plexus::io::security::attach_policy
{
public:
    bool decide(const plexus::io::security::attach_facts &) const noexcept override
    {
        return false;
    }
};

const refuse_all k_refuse_all{};

handshake_fsm_config refusing_cfg()
{
    handshake_fsm_config cfg = make_cfg(0x01);
    cfg.attach_policy        = &k_refuse_all;
    return cfg;
}

struct refused_acceptor : lone_session
{
    refused_acceptor()
            : lone_session(true, refusing_cfg())
    {
        hear(handshake_request_frame());
    }
};

struct torn_down_acceptor : established_acceptor
{
    torn_down_acceptor()
    {
        subject->tear_down();
        settle();
    }
};

constexpr std::array k_observable{wire::msg_type::subscribe, wire::msg_type::rpc_request, wire::msg_type::unidirectional, wire::msg_type::heartbeat, wire::msg_type::fetch_latched};

std::uint32_t observed(const lone_session &f, wire::msg_type type)
{
    switch(type)
    {
        case wire::msg_type::subscribe:
            return f.fanned_out() ? 1 : 0;
        case wire::msg_type::rpc_request:
            return f.fx.handler_runs;
        case wire::msg_type::unidirectional:
            return f.fx.messages;
        case wire::msg_type::heartbeat:
            return f.fx.stamps;
        default:
            return f.fx.peer_frames;
    }
}

bool saw(const lone_session &f, lifecycle_edge edge)
{
    return std::ranges::find(f.edges, edge) != f.edges.end();
}

void check_quiet(const lone_session &f)
{
    CHECK(f.fx.stamps == 0);
    CHECK(f.fx.messages == 0);
    CHECK(f.fx.peer_frames == 0);
    CHECK(f.fx.handler_runs == 0);
    CHECK_FALSE(f.fanned_out());
}

plexus::io::detail::drop_cause expected_drop(wire::msg_type type, bool torn_down)
{
    const bool unused = type == wire::msg_type::bidirectional || type == wire::msg_type::fetch_metadata;
    return unused && !torn_down ? plexus::io::detail::drop_cause::malformed : plexus::io::detail::drop_cause::unestablished;
}

template<typename Fixture>
void check_every_type_quiet(bool torn_down)
{
    for(const wire::msg_type type : k_all_types)
    {
        if(is_handshake(type))
            continue;
        Fixture f;
        const plexus::node_id before = f.subject->peer_identity();
        INFO("msg_type " << static_cast<std::uint32_t>(type));
        f.inject(frame_for(type));
        check_quiet(f);
        CHECK(f.fx.drops == std::vector{expected_drop(type, torn_down)});
        CHECK_FALSE(f.subject->is_complete());
        CHECK(f.subject->peer_identity() == before);
    }
}

}

TEST_CASE("session gate: an accepted session admits only handshake frames before its handshake", "[integration][session_gate]")
{
    check_every_type_quiet<pre_handshake_acceptor>(false);
}

TEST_CASE("session gate: a dialed session admits only handshake frames before the response", "[integration][session_gate]")
{
    check_every_type_quiet<pre_response_dialer>(false);
}

TEST_CASE("session gate: a session refused at attach runs no consumer", "[integration][session_gate]")
{
    const refused_acceptor probe;
    REQUIRE(saw(probe, lifecycle_edge::rejected));
    REQUIRE_FALSE(probe.subject->is_complete());
    check_every_type_quiet<refused_acceptor>(true);
}

TEST_CASE("session gate: a torn-down session runs no consumer", "[integration][session_gate]")
{
    const torn_down_acceptor probe;
    REQUIRE(saw(probe, lifecycle_edge::disconnected));
    check_every_type_quiet<torn_down_acceptor>(true);
}

TEST_CASE("session gate: an established session runs every observable consumer", "[integration][session_gate]")
{
    for(const wire::msg_type type : k_observable)
    {
        established_acceptor f;
        REQUIRE(f.subject->is_complete());
        INFO("msg_type " << static_cast<std::uint32_t>(type));
        f.inject(frame_for(type));
        CHECK(observed(f, type) == 1);
        CHECK(f.fx.drops.empty());
        CHECK(f.subject->is_complete());
    }
}
