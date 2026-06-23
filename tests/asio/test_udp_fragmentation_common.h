// Live-loopback large-payload fragmentation legs over real sockets: a payload larger
// than the per-datagram budget is SPLIT into numbered fragments at the channel and
// reassembled into ONE message at the peer. The exhaustive deterministic interleavings
// are covered sans-IO elsewhere; these prove the live composition. The ctest invocation
// is re-run across >=3 process runs for cross-process reproducibility (a live-networking
// claim is never made from one run).
#pragma once

#include "plexus/asio/udp_channel.h"
#include "plexus/asio/udp_server.h"
#include "plexus/asio/udp_transport.h"

#include "plexus/wire/udp_ack.h"
#include "plexus/wire/udp_envelope.h"

#include "plexus/io/fragmentation.h"

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

namespace udp_fragmentation_fixture {

namespace pasio = plexus::asio;
namespace wire  = plexus::wire;
namespace pio   = plexus::io;

using ms = std::chrono::milliseconds;

constexpr pasio::udp_transport::arq_type::schedule fast_hs{ms{20}, ms{40}, ms{80}};

// A compressed ARQ config so the fragment-loss retransmit is quick. A generous window +
// retransmit cap so a multi-hundred-fragment reliable message rides through the bounded
// congestion=block queue without exhausting retransmits over loopback.
inline plexus::datagram::detail::udp_arq_config fast_arq()
{
    return plexus::datagram::detail::udp_arq_config{.window         = 256,
                                                    .initial_rto    = ms{20},
                                                    .min_rto        = ms{10},
                                                    .max_rto        = ms{120},
                                                    .max_retransmit = 20};
}

// A deterministic, position-dependent payload so a reassembled message is byte-checked
// against a regenerated oracle rather than a memcmp of two live buffers.
inline std::vector<std::byte> make_payload(std::size_t n, std::uint8_t salt)
{
    std::vector<std::byte> out(n);
    for(std::size_t i = 0; i < n; ++i)
        out[i] = static_cast<std::byte>((i * 31u + salt * 7u + (i >> 8)) & 0xFFu);
    return out;
}

inline bool equal_bytes(std::span<const std::byte> a, std::span<const std::byte> b)
{
    if(a.size() != b.size())
        return false;
    for(std::size_t i = 0; i < a.size(); ++i)
        if(a[i] != b[i])
            return false;
    return true;
}

enum class action
{
    pass,
    drop
};

template<typename Pred>
void pump_until(::asio::io_context &io, Pred pred, ms timeout = ms{8000})
{
    auto bound = std::chrono::steady_clock::now() + timeout;
    while(!pred() && std::chrono::steady_clock::now() < bound)
    {
        io.poll();
        if(io.stopped())
            io.restart();
    }
}

} // namespace udp_fragmentation_fixture
