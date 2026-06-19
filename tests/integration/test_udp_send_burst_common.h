#ifndef HPP_GUARD_TESTS_INTEGRATION_UDP_SEND_BURST_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_UDP_SEND_BURST_COMMON_H

// The outbound-send-lifetime gate: the shared udp_server owns each in-flight datagram
// across its async_send_to (a serial owned queue), so a BURST of sends issued in ONE
// turn — without draining the io_context between them — each transmits its OWN bytes,
// not a neighbor's reused scratch. This is the reproducibility proof for the buffer-
// lifetime defect: against a sink that referenced a caller-owned, immediately-reused
// scratch buffer across the async op, every leg below would transmit corrupted or
// torn bytes (the LAST scratch content), so the test would FAIL. Three legs:
//   * best_effort burst: N datagrams of DISTINCT, size-varying payloads queued in one
//     turn (no drain between sends) all arrive intact and in publish order.
//   * two-peer overlap: two channels over the SAME server each send in the same turn —
//     neither peer's in-flight datagram is overwritten by the other's.
//   * reliable retransmit vs fresh send: a lossy relay forces an ARQ retransmit to
//     interleave with fresh submits; every reliable frame still arrives in order.
// Looped 100x in-body; the ctest invocation is re-run across >=3 process runs (a
// transport/timing claim is never made from a single run).

#include "plexus/asio/udp_channel.h"
#include "plexus/asio/udp_server.h"
#include "plexus/asio/udp_transport.h"

#include "plexus/wire/udp_ack.h"
#include "plexus/wire/udp_envelope.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/buffer.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/udp.hpp>

#include <array>
#include <deque>
#include <chrono>
#include <memory>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <algorithm>

namespace pasio = plexus::asio;
namespace wire  = plexus::wire;
namespace pio   = plexus::io;

namespace udp_send_burst_fixture {

using ms = std::chrono::milliseconds;

constexpr pasio::udp_transport::arq_type::schedule fast_hs{ms{20}, ms{40}, ms{80}};

inline pio::detail::udp_arq_config fast_arq()
{
    return pio::detail::udp_arq_config{.window         = 64,
                                       .initial_rto    = ms{20},
                                       .min_rto        = ms{10},
                                       .max_rto        = ms{80},
                                       .max_retransmit = 12};
}

inline std::vector<std::byte> bytes_of(const std::string &s)
{
    std::vector<std::byte> out(s.size());
    for(std::size_t i = 0; i < s.size(); ++i)
        out[i] = static_cast<std::byte>(s[i]);
    return out;
}

inline std::string str_of(std::span<const std::byte> b)
{
    std::string s(b.size(), '\0');
    for(std::size_t i = 0; i < b.size(); ++i)
        s[i] = static_cast<char>(std::to_integer<unsigned char>(b[i]));
    return s;
}

template<typename Pred>
inline void pump_until(::asio::io_context &io, Pred pred, ms timeout = ms{6000})
{
    auto bound = std::chrono::steady_clock::now() + timeout;
    while(!pred() && std::chrono::steady_clock::now() < bound)
    {
        io.poll();
        if(io.stopped())
            io.restart();
    }
}

// A listen+dial best_effort pair on one io_context (handshake established).
struct pair_fixture
{
    ::asio::io_context   io;
    pasio::udp_transport server{io};
    pasio::udp_transport client{io, pasio::udp_channel::default_max_payload, fast_hs};

    std::unique_ptr<pasio::udp_channel> accepted, dialed;
    std::vector<std::string>            received;

    pair_fixture()
    {
        server.on_accepted(
                [this](std::unique_ptr<pasio::udp_channel> ch)
                {
                    accepted = std::move(ch);
                    accepted->on_data([this](std::span<const std::byte> b)
                                      { received.push_back(str_of(b)); });
                });
        server.listen({"udp", "127.0.0.1:0"});
        pump_until(io, [this] { return server.port() != 0; });

        client.on_dialed([this](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &)
                         { dialed = std::move(ch); });
        client.dial({"udp", "127.0.0.1:" + std::to_string(server.port())});
        pump_until(io, [this] { return dialed && accepted; });
    }
};

}

#endif
