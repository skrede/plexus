#ifndef HPP_GUARD_TESTS_INTEGRATION_SESSION_MALFORMED_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_SESSION_MALFORMED_COMMON_H

#include "test_session_gate_common.h"
#include "test_routing_engine_inproc_common.h"

#include "plexus/io/observer.h"

#include "plexus/io/detail/drop_event.h"

#include "plexus/log/logger.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/handshake.h"

#include <memory>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <algorithm>
#include <string_view>

namespace session_malformed_fixture {

using namespace session_gate_fixture;
using engine_clock = routing_inproc_fixture::manual_clock;

inline constexpr std::string_view k_malformed_line = "plexus: peer_session malformed_frame";

inline std::vector<std::byte> undecodable_subscribe()
{
    return frame_of(wire::msg_type::subscribe, {});
}

inline std::vector<std::byte> handshake_naming(std::uint8_t id_seed)
{
    const wire::handshake_request req{.id                       = make_cfg(id_seed).self_id,
                                      .version_major            = 1,
                                      .version_minor            = 0,
                                      .compatible_version_major = 1,
                                      .compatible_version_minor = 0,
                                      .protocol_version         = wire::k_protocol_version,
                                      .fingerprint              = 0,
                                      .key_id                   = {},
                                      .own_nonce                = {},
                                      .cipher_offer             = 0,
                                      .chosen_cipher            = 0,
                                      .proof                    = {}};
    return frame_of(wire::msg_type::handshake_req, wire::encode_handshake_request(req));
}

struct drop_recorder final : plexus::io::observer
{
    void on_drop(const plexus::io::detail::drop_event &ev) override
    {
        seen.push_back(ev.cause);
    }

    std::vector<plexus::io::detail::drop_cause> seen;
};

struct recording_logger final : plexus::log::logger
{
    void warn(std::string_view message) override
    {
        lines.emplace_back(message);
    }

    std::vector<std::string> malformed_lines() const
    {
        std::vector<std::string> out;
        std::ranges::copy_if(lines, std::back_inserter(out), [](const std::string &line) { return line.starts_with(k_malformed_line); });
        return out;
    }

    std::vector<std::string> lines;
};

// An engine accepting a bare channel the test speaks for, so the frames the engine's session hears are the test's.
struct engine_with_raw_peer
{
    engine_with_raw_peer()
            : bus()
            , ex(bus)
            , transport(ex, bus)
            , raw(ex, bus)
            , logger()
            , node(transport, ex, routing_inproc_fixture::make_cfg(0xA1), routing_inproc_fixture::k_long_timeout, routing_inproc_fixture::forever_cfg(), routing_inproc_fixture::k_seed,
                   logger)
            , channel()
    {
        node.listen({"inproc", "node-a"});
        raw.on_dialed([this](std::unique_ptr<inproc_channel<engine_clock>> ch, const plexus::io::endpoint &) { channel = std::move(ch); });
        raw.dial({"inproc", "node-a"});
        ex.drain();
    }

    inproc_bus<engine_clock> bus;
    inproc_executor<engine_clock> ex;
    routing_inproc_fixture::transport_t transport;
    routing_inproc_fixture::transport_t raw;
    recording_logger logger;
    routing_inproc_fixture::engine node;
    std::unique_ptr<inproc_channel<engine_clock>> channel;
};

}

#endif
