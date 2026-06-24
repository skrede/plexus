#ifndef HPP_GUARD_PLEXUS_IO_SESSION_BUILD_CONTEXT_H
#define HPP_GUARD_PLEXUS_IO_SESSION_BUILD_CONTEXT_H

#include "plexus/policy.h"

#include "plexus/detail/compat.h"

#include "plexus/io/observer.h"
#include "plexus/io/message_info.h"
#include "plexus/io/handshake_fsm.h"
#include "plexus/io/security_seam.h"
#include "plexus/io/security_event.h"
#include "plexus/io/lifecycle_event.h"
#include "plexus/io/reconnect_config.h"
#include "plexus/io/message_forwarder.h"
#include "plexus/io/procedure_forwarder.h"

#include "plexus/log/logger.h"

#include <span>
#include <chrono>
#include <cstdint>
#include <string_view>

namespace plexus::io {

template<typename Policy>
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
    plexus::detail::move_only_function<void(std::string_view, std::span<const std::byte>, const message_info &)> on_message;
    plexus::detail::move_only_function<void(std::string_view, const object_carrier &)> on_object;
    plexus::detail::move_only_function<void(const lifecycle_event &)> on_lifecycle;
    plexus::detail::move_only_function<void(const node_id &)> on_stamp_seen;
    observer &session_observer;
    security_seam install_security;
    // Fail-closed: a security-engaged accept with no factory wired is refused, never fail-open.
    plexus::detail::move_only_function<void(typename Policy::byte_channel_type &, const security_negotiation &)> install_security_factory;
};

}

#endif
