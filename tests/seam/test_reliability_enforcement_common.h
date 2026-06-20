#ifndef HPP_GUARD_PLEXUS_TESTS_SEAM_TEST_RELIABILITY_ENFORCEMENT_COMMON_H
#define HPP_GUARD_PLEXUS_TESTS_SEAM_TEST_RELIABILITY_ENFORCEMENT_COMMON_H

// The permissive/strict reliability enforcement gate on the routing_engine subscribe
// demand path, as a deterministic seam test over the inproc backend. The gate is a
// PRE-DIAL decision read from the endpoint scheme the engine already holds (known_peers)
// — it mirrors the locality confinement gate exactly: an admitted demand reaches the
// peer and (over a real inproc rendezvous) COMPLETES a session; a refused demand
// establishes NO path (no slot, no demand, no dial), so no session ever forms.

#include "plexus/io/routing_engine.h"
#include "plexus/io/handshake_fsm.h"
#include "plexus/io/reconnect_config.h"
#include "plexus/io/reliability_requirement.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_timer.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/node_id.h"
#include "plexus/policy.h"

#include "plexus/detail/compat.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <chrono>
#include <cstdint>

namespace reliability_enforcement_fixture {

using plexus::io::endpoint;
using plexus::io::locality;
using plexus::io::reconnect_config;
using plexus::io::handshake_fsm_config;
using plexus::io::reliability_requirement;

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_timer;
using plexus::inproc::inproc_channel;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_transport;

struct manual_clock
{
    using duration                  = std::chrono::steady_clock::duration;
    using rep                       = duration::rep;
    using period                    = duration::period;
    using time_point                = std::chrono::time_point<manual_clock>;
    static constexpr bool is_steady = false;

    static inline time_point current{};
    static time_point        now() noexcept { return current; }
};

struct manual_policy
{
    using executor_type     = inproc_executor<manual_clock> &;
    using byte_channel_type = inproc_channel<manual_clock>;
    using timer_type        = inproc_timer<manual_clock>;
    using byte_owner        = std::shared_ptr<const void>;

    static void post(executor_type ex, plexus::detail::move_only_function<void()> fn)
    {
        ex.post(std::move(fn));
    }
};

static_assert(plexus::Policy<manual_policy>);

using transport_t = inproc_transport<manual_clock>;
using engine      = plexus::io::routing_engine<manual_policy, transport_t, manual_clock>;

constexpr auto          k_long_timeout = std::chrono::hours(1);
constexpr std::uint64_t k_seed         = 0xC0FFEEu;

inline handshake_fsm_config make_cfg(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return handshake_fsm_config{.self_id                  = id,
                                .version_major            = 1,
                                .version_minor            = 0,
                                .compatible_version_major = 1,
                                .compatible_version_minor = 0};
}

inline plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

inline reconnect_config forever_cfg()
{
    return reconnect_config{std::chrono::milliseconds(100), std::chrono::milliseconds(10000),
                            std::nullopt, std::nullopt};
}

// A two-node inproc rendezvous: a dialer engine (A) and a responder engine (B) on one
// bus/executor. B listens under a chosen SCHEME (the gate's discriminator + the bus key);
// A learns the peer at that same endpoint and subscribes with a chosen requirement. An
// admitted demand genuinely connects (has_session becomes true after drive()); a refused
// one never reaches, so no session forms. Member order: bus/executor/transports BEFORE
// the engines so teardown unwinds the engines' channels first.
struct rendezvous
{
    inproc_bus<manual_clock>      bus;
    inproc_executor<manual_clock> ex{bus};
    transport_t                   dialer_tp{ex, bus};
    transport_t                   responder_tp{ex, bus};

    engine dialer{dialer_tp, ex, make_cfg(0xA1), k_long_timeout, forever_cfg(), k_seed, false};
    engine responder{responder_tp,  ex,     make_cfg(0xB2), k_long_timeout,
                     forever_cfg(), k_seed, false};

    plexus::node_id peer{make_id(0xB2)};
    endpoint        peer_ep;

    // Stand the responder up under `scheme` so the bus keys its listener there and the
    // gate classifies the peer's reliability from it; teach the dialer the same endpoint.
    explicit rendezvous(const std::string &scheme)
            : peer_ep{scheme, "responder"}
    {
        responder.listen(peer_ep);
        dialer.note_peer(peer, peer_ep);
    }

    void drive() { ex.drain(); }
    bool connected()
    {
        drive();
        return dialer.has_session(peer);
    }
};

}

#endif
