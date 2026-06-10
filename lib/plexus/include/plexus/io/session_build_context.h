#ifndef HPP_GUARD_PLEXUS_IO_SESSION_BUILD_CONTEXT_H
#define HPP_GUARD_PLEXUS_IO_SESSION_BUILD_CONTEXT_H

#include "plexus/io/handshake_fsm.h"
#include "plexus/io/security_event.h"
#include "plexus/io/security_seam.h"
#include "plexus/io/lifecycle_event.h"
#include "plexus/io/reconnect_config.h"
#include "plexus/io/message_forwarder.h"
#include "plexus/io/procedure_forwarder.h"

#include "plexus/policy.h"

#include "plexus/log/logger.h"

#include "plexus/detail/compat.h"

#include <chrono>
#include <cstdint>

namespace plexus::io {

// The node-shared inputs every per-slot session is built from: the engine owns one
// of these and the registry borrows it by reference. A slot rebuild (a reconnect)
// draws the same forwarders, self identity, handshake bound, executor and logger
// from here, so the node-wide wiring is reused with no re-plumbing.
template <typename Policy>
struct session_build_context
{
    using executor_type = typename Policy::executor_type;

    executor_type executor;
    handshake_fsm_config fsm_cfg;
    std::chrono::nanoseconds handshake_timeout;
    message_forwarder<Policy> &messages;
    procedure_forwarder<Policy> &procedures;
    reconnect_config redial;
    std::uint64_t redial_seed;
    log::logger &logger;
    // The node-shared route from any slot's session up to the engine's observer
    // fan-out: the engine sets this after construction, the registry forwards each
    // session's lifecycle edge through it. Absent (unset) on a context built before
    // the engine wires it — a slot's forward guards on it being set.
    plexus::detail::move_only_function<void(const lifecycle_event &)> on_lifecycle;
    // The node-shared presence-stamp route from any slot's session to the engine's one
    // liveliness monitor: the engine sets this after construction to monitor.stamp_seen,
    // the registry wires each session's on_stamp_seen through it carrying that session's
    // pinned peer id. Absent (unset) until the engine wires it — the session guards on it.
    plexus::detail::move_only_function<void(const node_id &)> on_stamp_seen;
    // The node-shared security-event route, shaped exactly like on_lifecycle: the engine
    // sets it after construction, the registry forwards each session's security event
    // through it. The single node-level attach_policy that gates every transport rides
    // fsm_cfg.attach_policy (one per node, injected at spine construction — null is the
    // explicit accept-any default). Absent (unset) until the engine wires it.
    plexus::detail::move_only_function<void(const security_event &)> on_security;
    // The node-shared, OpenSSL-free security seam (transcript digest + AEAD decorator
    // install), injected once at spine construction. It is the type-erased boundary that
    // keeps the EVP/decorator instantiation behind the gated target while the bridge
    // stays plaintext: an empty seam is the no-AEAD posture. The registry threads it to
    // each built session.
    security_seam install_security;
    // The production source of each session's AEAD decorator-install action: the gated
    // layer sets this once at spine construction; given a just-built channel and the
    // negotiation it derives the keys and routes the channel through the EVP decorator.
    // The registry binds it per session into peer_session::on_install_security, capturing
    // that slot's channel. Type-erased so the core bridge links no libcrypto. Absent
    // (unset) until the gated transport path is wired — a security-engaged accept with no
    // factory then finds no per-session hook and is refused fail-closed (never fail-open).
    plexus::detail::move_only_function<
            void(typename Policy::byte_channel_type &, const security_negotiation &)>
            install_security_factory;
};

}

#endif
