#ifndef HPP_GUARD_PLEXUS_TESTS_IO_SLOT_REBUILD_FIXTURE_H
#define HPP_GUARD_PLEXUS_TESTS_IO_SLOT_REBUILD_FIXTURE_H

#include "slot_channel.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/observer.h"
#include "plexus/io/handshake_fsm.h"
#include "plexus/io/reconnect_config.h"
#include "plexus/io/message_forwarder.h"
#include "plexus/io/procedure_forwarder.h"
#include "plexus/io/peer_session_registry.h"
#include "plexus/io/session_build_context.h"

#include "plexus/log/logger.h"

#include "plexus/detail/compat.h"

#include "support/race_timer.h"

#include <deque>
#include <chrono>
#include <memory>
#include <cstddef>
#include <utility>

namespace plexus::testing {

struct race_policy
{
    using executor_type     = race_executor &;
    using byte_channel_type = slot_channel;
    using timer_type        = race_timer;
    using byte_owner        = std::shared_ptr<const void>;

    static void post(executor_type ex, plexus::detail::move_only_function<void()> fn)
    {
        ex.post(std::move(fn));
    }
};

struct dial_transport
{
    void listen(const io::endpoint &)
    {
    }

    void dial(const io::endpoint &)
    {
    }

    void close()
    {
    }

    void on_accepted(plexus::detail::move_only_function<void(std::unique_ptr<slot_channel>)>)
    {
    }

    void on_error(plexus::detail::move_only_function<void(io::io_error)>)
    {
    }

    void on_dial_failed(plexus::detail::move_only_function<void(const io::endpoint &, io::io_error)>)
    {
    }

    void on_dialed(plexus::detail::move_only_function<void(std::unique_ptr<slot_channel>, const io::endpoint &)>)
    {
    }
};

inline io::handshake_fsm_config one_retry_fsm()
{
    return {.self_id = node_id{std::byte{0x51}}, .version_major = 1, .version_minor = 0, .compatible_version_major = 1, .compatible_version_minor = 0, .handshake_retry = 1};
}

// One dialed slot of the production registry over the race harness. Every dial completion the test hands over
// arrives through the registry's dial-completion entry, as the routing engine delivers it.
struct slot_rebuild_fixture
{
    using registry_type = io::peer_session_registry<race_policy, dial_transport>;

    slot_rebuild_fixture()
            : executor()
            , transport()
            , logger()
            , observer()
            , messages(logger)
            , procedures(executor, std::chrono::hours(1), logger)
            , build{.executor                 = executor,
                    .fsm_cfg                  = one_retry_fsm(),
                    .handshake_timeout        = std::chrono::seconds(1),
                    .messages                 = messages,
                    .procedures               = procedures,
                    .redial                   = io::reconnect_config{std::chrono::milliseconds(10), std::chrono::milliseconds(100), std::nullopt, std::nullopt},
                    .redial_seed              = 1,
                    .logger                   = logger,
                    .on_message               = {},
                    .on_object                = {},
                    .on_lifecycle             = {},
                    .on_stamp_seen            = {},
                    .on_decline_seen          = {},
                    .session_observer         = observer,
                    .install_security         = {},
                    .install_security_factory = {}}
            , probes()
            , registry(transport, build)
            , peer{std::byte{0x52}}
            , dialed{"tcp", "198.51.100.7:4100"}
    {
        registry.ensure_slot(peer, dialed, "peer-node");
    }

    slot_channel &hand()
    {
        auto channel        = std::make_unique<slot_channel>(executor, probes.emplace_back());
        slot_channel &taken = *channel;
        registry.build_session_for_endpoint(dialed, std::move(channel));
        return taken;
    }

    channel_probe &probe(std::size_t n)
    {
        return probes.at(n);
    }

    race_executor executor;
    dial_transport transport;
    log::null_logger logger;
    io::observer observer;
    io::message_forwarder<race_policy> messages;
    io::procedure_forwarder<race_policy> procedures;
    io::session_build_context<race_policy> build;
    // Declared before the registry, so every probe outlives the channel that writes to it.
    std::deque<channel_probe> probes;
    registry_type registry;
    node_id peer;
    io::endpoint dialed;
};

}

#endif
