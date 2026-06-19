#ifndef HPP_GUARD_PLEXUS_IO_SESSION_BUILD_CONTEXT_H
#define HPP_GUARD_PLEXUS_IO_SESSION_BUILD_CONTEXT_H

#include "plexus/io/observer.h"
#include "plexus/io/message_info.h"
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

#include <span>
#include <chrono>
#include <cstdint>
#include <string_view>

namespace plexus::io {

// The node-shared inputs every per-slot session is built from: the engine owns one, the
// registry borrows it. A slot rebuild (a reconnect) draws the same wiring from here, so the
// node-wide plumbing is reused with no re-plumbing.
template<typename Policy>
struct session_build_context
{
    using executor_type = typename Policy::executor_type;

    executor_type                executor;
    handshake_fsm_config         fsm_cfg;
    std::chrono::nanoseconds     handshake_timeout;
    message_forwarder<Policy>   &messages;
    procedure_forwarder<Policy> &procedures;
    reconnect_config             redial;
    std::uint64_t                redial_seed;
    log::logger                 &logger;
    // The receive route. The per-session seam is lost on a rebuild and raced by the posted
    // observer fan-out, so it is threaded from here on every build. One 3-arg shape carrying
    // the message_info serves both arities (a bytes-only consumer drops the info). Absent
    // until the engine wires it — a session guards on it.
    plexus::detail::move_only_function<void(std::string_view, std::span<const std::byte>,
                                            const message_info &)>
            on_message;
    // The process-tier object-lane route, shaped like on_message. Absent until wired.
    plexus::detail::move_only_function<void(std::string_view, const object_carrier &)> on_object;
    // A typed lifecycle sink, NOT an observer per-edge method: the engine arms its liveliness
    // monitor off the edge before fanning the per-edge observer method out posted, so the edge
    // must reach it typed first. Absent until wired — a slot's forward guards on it.
    plexus::detail::move_only_function<void(const lifecycle_event &)> on_lifecycle;
    // The presence-stamp route to the engine's one liveliness monitor, carrying the session's
    // pinned peer id. Absent until wired — the session guards on it.
    plexus::detail::move_only_function<void(const node_id &)> on_stamp_seen;
    // The security edge routes straight into session_observer.on_security (the lifecycle edge
    // keeps its typed sink above because the engine arms the monitor off it first). The engine
    // installs a posting adapter here so a security transition reaches observers POSTED, never
    // inline. Default = the inert observer (one predictable branch when none is set).
    observer &session_observer = shared_null_observer();
    // The OpenSSL-free security seam (transcript digest + AEAD decorator install): the
    // type-erased boundary that keeps the EVP/decorator instantiation behind the gated target
    // while the bridge stays plaintext. An empty seam is the no-AEAD posture.
    security_seam install_security;
    // Derives the keys and routes a just-built channel through the EVP decorator. Type-erased
    // so the core bridge links no libcrypto. Absent until the gated path is wired — a
    // security-engaged accept with no factory is then refused fail-closed (never fail-open).
    plexus::detail::move_only_function<void(typename Policy::byte_channel_type &,
                                            const security_negotiation &)>
            install_security_factory;
};

}

#endif
