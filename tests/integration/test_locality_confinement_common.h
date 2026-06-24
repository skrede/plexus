#ifndef HPP_GUARD_TESTS_INTEGRATION_LOCALITY_CONFINEMENT_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_LOCALITY_CONFINEMENT_COMMON_H

// The locality confinement matrix: the delivery-tier QoS proven on BOTH enforcement
// sides. The DETERMINISTIC leg (always built, synthetic schemes, looped) drives the
// message_forwarder fan-out gate directly — subscribers tagged process("inproc"),
// local("unix"), and remote("tcp") attach to one forwarder, and per published reach
// mask ONLY the in-mask tiers receive the frame: a process-only topic reaches no
// channel; process|local never goes remote; remote never reaches a local/in-process
// peer; any reaches all. The synthetic "inproc" scheme is anchored to the production
// classifier (tier_of("inproc") == locality::process) so the process-tier proof cannot
// pass for the wrong reason. The LIVE leg (gated on the asio backend, looped, re-run
// across process runs) stands up a REAL AF_UNIX peer (local) and a REAL TCP peer
// (remote) and proves the same confinement over the wire. The DEMAND-GATE leg drives a
// routing_engine: a local-confined subscribe toward a tcp peer establishes NO remote
// path (no dial, no slot, is_connected stays false) — the symmetric demand-side half.

#include "plexus/io/locality.h"
#include "plexus/io/known_peers.h"
#include "plexus/io/routing_engine.h"
#include "plexus/io/handshake_fsm.h"
#include "plexus/io/reconnect_config.h"
#include "plexus/io/message_forwarder.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_timer.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/policy.h"
#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/detail/compat.h"

#include <catch2/catch_test_macros.hpp>

#ifdef PLEXUS_HAVE_ASIO_MUX
    #include "plexus/asio/asio_policy.h"
    #include "plexus/asio/asio_transport.h"
    #include "plexus/asio/asio_channel.h"
    #include "plexus/asio/unix_policy.h"
    #include "plexus/asio/unix_transport.h"
    #include "plexus/asio/unix_channel.h"

    #include "plexus/io/peer_session.h"
    #include "plexus/io/peer_context.h"

    #include <asio/io_context.hpp>

    #include <unistd.h>
#endif

#include <span>
#include <array>
#include <memory>
#include <chrono>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>
#include <system_error>

using plexus::io::locality;
using plexus::io::tier_of;

namespace locality_confinement_fixture {

inline std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

// A synthetic channel whose remote_endpoint() reports a TEST-CHOSEN scheme, so the
// forwarder's attach-time tier classification (tier_of(scheme)) is exercised
// deterministically — no backend, no socket. send() only counts; the bytes are
// irrelevant to a confinement (delivered-or-not) assertion.
struct tagged_executor
{
};

struct tagged_channel
{
    explicit tagged_channel(std::string scheme)
            : m_scheme(std::move(scheme))
    {
    }

    void send(std::span<const std::byte>)
    {
        ++sends;
    }
    void close()
    {
    }
    [[nodiscard]] plexus::io::endpoint remote_endpoint() const
    {
        return {m_scheme, ""};
    }
    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)>)
    {
    }
    void on_closed(plexus::detail::move_only_function<void()>)
    {
    }
    void on_error(plexus::detail::move_only_function<void(plexus::io::io_error)>)
    {
    }
    void on_protocol_close(plexus::detail::move_only_function<void(plexus::wire::close_cause)>)
    {
    }

    std::string m_scheme;
    std::size_t sends{0};
};

struct tagged_timer
{
    explicit tagged_timer(tagged_executor &)
    {
    }
    tagged_timer(tagged_executor &, std::error_code &)
    {
    }
    void expires_after(std::chrono::milliseconds)
    {
    }
    void async_wait(plexus::detail::move_only_function<void(std::error_code)>)
    {
    }
    void cancel()
    {
    }
};

struct tagged_policy
{
    using executor_type     = tagged_executor &;
    using byte_channel_type = tagged_channel;
    using timer_type        = tagged_timer;
    using byte_owner        = std::shared_ptr<const void>;

    static void post(executor_type, plexus::detail::move_only_function<void()> fn)
    {
        fn();
    }
};

static_assert(plexus::Policy<tagged_policy>);

inline plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

inline plexus::io::handshake_fsm_config make_cfg(std::uint8_t seed)
{
    return plexus::io::handshake_fsm_config{.self_id = make_id(seed), .version_major = 1, .version_minor = 0, .compatible_version_major = 1, .compatible_version_minor = 0};
}

inline plexus::io::reconnect_config forever_cfg()
{
    return plexus::io::reconnect_config{std::chrono::milliseconds(100), std::chrono::milliseconds(10000), std::nullopt, std::nullopt};
}

}

#endif
