#ifndef HPP_GUARD_TESTS_INTEGRATION_PROVEN_IDENTITY_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_PROVEN_IDENTITY_COMMON_H

#include "liveliness_inproc_common.h"

#include "plexus/io/observer.h"
#include "plexus/io/peer_kind.h"
#include "plexus/io/handshake_protocol.h"

#include "plexus/io/detail/drop_event.h"

#include "plexus/log/logger.h"

#include "plexus/node_id.h"

#include <chrono>
#include <string>
#include <vector>
#include <cstdint>
#include <string_view>

namespace proven_identity_fixture {

using namespace liveliness_inproc;
using plexus::node_id;

constexpr std::string_view k_topic{"proven"};
constexpr auto k_step   = std::chrono::milliseconds(100);
constexpr auto k_redial = std::chrono::seconds(3);

bool provisional(const node_id &id);
handshake_fsm_config older_cfg(std::uint8_t id_seed);

struct id_recorder final : plexus::io::observer
{
    id_recorder();

    void on_peer_connected(const node_id &id, std::string_view, plexus::io::peer_kind) override;
    void on_peer_disconnected(const node_id &id, std::string_view, plexus::io::peer_kind) override;
    void on_peer_reconnected(const node_id &id, std::string_view, plexus::io::peer_kind) override;
    void on_peer_ready(const node_id &id, std::string_view, plexus::io::peer_kind) override;
    void on_peer_rejected(const node_id &id, std::string_view name, plexus::io::handshake_outcome) override;
    void on_peer_liveliness(const peer_liveliness_event &ev) override;
    void on_drop(const plexus::io::detail::drop_event &ev) override;
    bool observes_liveliness() const override;

    std::vector<node_id> ids;
    std::vector<node_id> verdicts;
    std::vector<node_id> rejected;
    std::vector<std::string> rejected_names;
};

// a dials b by its card and b never dials a, so every session b holds for a is an accepted one.
struct proven_net
{
    proven_net();

    void advance_by(std::chrono::nanoseconds total);
    void connect();
    void publish();
    engine::session_type *accepted_from_a();
    void drop_and_redial();

    node_id id_a;
    node_id id_b;
    endpoint ep_a;
    endpoint ep_b;
    inproc_bus<manual_clock> bus;
    inproc_executor<manual_clock> ex;
    transport_t transport_a;
    transport_t transport_b;
    plexus::log::null_logger sink;
    id_recorder seen;
    std::vector<node_id> sources;
    engine a;
    engine b;
};

}

#endif
