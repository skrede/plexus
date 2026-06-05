#ifndef HPP_GUARD_PLEXUS_IO_SESSION_BUILD_CONTEXT_H
#define HPP_GUARD_PLEXUS_IO_SESSION_BUILD_CONTEXT_H

#include "plexus/io/handshake_fsm.h"
#include "plexus/io/reconnect_config.h"
#include "plexus/io/message_forwarder.h"
#include "plexus/io/procedure_forwarder.h"

#include "plexus/policy.h"

#include "plexus/log/logger.h"

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
};

}

#endif
