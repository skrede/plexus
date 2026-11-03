#ifndef HPP_GUARD_TESTS_INTEGRATION_CROSSED_DIAL_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_CROSSED_DIAL_COMMON_H

#include "recording_observer.h"
#include "liveliness_inproc_common.h"

#include "plexus/io/liveness_event.h"
#include "plexus/io/subscriber_qos.h"

#include "plexus/log/logger.h"

#include "plexus/node_id.h"

#include <span>
#include <chrono>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

namespace crossed_dial_fixture {

using namespace liveliness_inproc;
using plexus::node_id;

constexpr auto k_window = std::chrono::seconds(2);
constexpr auto k_step   = std::chrono::milliseconds(100);
constexpr std::string_view k_topic{"crossed"};

enum class crossing : std::uint8_t
{
    at_once,
    newcomer_loses,
    incumbent_loses
};

struct recording_logger final : plexus::log::logger
{
    void warn(std::string_view line) override
    {
        lines.emplace_back(line);
    }

    std::vector<std::string> lines;
};

std::string hex_of(const node_id &id);
std::vector<std::string> resolution_lines(const recording_logger &log);
std::size_t complete_sessions_for(engine &e, const node_id &proven);
std::optional<node_id> kept_slot(engine &e, const node_id &proven);
int verdicts_for(const recording_liveliness_observer &obs, const node_id &id, std::size_t from);

struct node_side
{
    node_side(transport_t &transport, inproc_executor<manual_clock> &ex, std::uint8_t seed, const liveliness_options &live);

    recording_logger log;
    recording_observer edges;
    std::vector<std::string> received;
    recording_liveliness_observer verdicts;
    std::vector<plexus::io::liveness_event> liveness;
    engine e;
};

// The higher id is a's, so a keeps the session it dialed and b the one it accepted.
struct crossed_net
{
    explicit crossed_net(const liveliness_options &live = liveliness_options{});

    void drive();
    void advance_by(std::chrono::nanoseconds total);
    void cross(crossing order, const plexus::io::subscriber_qos &qos = {});
    void publish_from(node_side &side);

    node_id id_a;
    node_id id_b;
    node_id id_c;
    endpoint ep_a;
    endpoint ep_b;
    endpoint ep_c;
    inproc_bus<manual_clock> bus;
    inproc_executor<manual_clock> ex;
    transport_t transport_a;
    transport_t transport_b;
    transport_t transport_c;
    node_side a;
    node_side b;
    node_side c;
};

}

#endif
