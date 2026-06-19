#ifndef HPP_GUARD_TESTS_INTEGRATION_UDP_LARGE_PAYLOAD_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_UDP_LARGE_PAYLOAD_COMMON_H

// Large-payload datagram capability over real loopback sockets: a 1 MB and a 4 MB
// message round-trip byte-identically through the existing fragmenter + per-channel
// reassembler over BOTH the best-effort ("udp") and the reliable-ARQ ("udpr") legs,
// looped in-body and re-run across >=3 process runs (a transport claim is never made
// from a single run). The deterministic loss/reorder shim (tests/support) drives the
// injected-loss legs: under a fixed loss fraction the best-effort path drops the WHOLE
// message on a lost fragment (drop-whole-message semantics, recorded as a measured
// observation) while the reliable path reassembles via retransmit. The shim's own
// determinism (a byte-identical drop/reorder sequence across two runs) is asserted
// first — the empirical-reproducibility property the fragment-scale sweep depends on.
//
// No plexus source is touched by this test; the loss is injected at the wire boundary.

#include "plexus/asio/udp_channel.h"
#include "plexus/asio/udp_server.h"
#include "plexus/asio/udp_transport.h"

#include "plexus/io/fragmentation.h"

#include "plexus/wire/udp_envelope.h"

#include "support/loss_reorder_shim.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/buffer.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/udp.hpp>

#include <array>
#include <chrono>
#include <memory>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <algorithm>

namespace pasio = plexus::asio;
namespace pio   = plexus::io;
namespace pwire = plexus::wire;
namespace ptest = plexus::testing;

namespace udp_large_payload_fixture {

using ms = std::chrono::milliseconds;

constexpr pasio::udp_transport::arq_type::schedule fast_hs{ms{20}, ms{40}, ms{80}};

// A generous window + quick retransmit so a multi-hundred-fragment reliable message rides
// through the bounded congestion=block queue without exhausting retransmits over loopback.
inline pio::detail::udp_arq_config large_arq()
{
    return pio::detail::udp_arq_config{.window         = 1024,
                                       .initial_rto    = ms{20},
                                       .min_rto        = ms{10},
                                       .max_rto        = ms{160},
                                       .max_retransmit = 40};
}

// A DELIBERATELY tiny send window so a many-fragment message has far more fragments than
// the window admits: the bulk of fragments must transit the bounded congestion=block
// backpressure queue, which is the path that must preserve each fragment's FRAGMENTED
// envelope bit (a window-sized message never parks a fragment and would not exercise it).
inline pio::detail::udp_arq_config tiny_window_arq()
{
    return pio::detail::udp_arq_config{.window         = 8,
                                       .initial_rto    = ms{20},
                                       .min_rto        = ms{10},
                                       .max_rto        = ms{160},
                                       .max_retransmit = 40};
}

// A deterministic, position-dependent payload byte-checked against a regenerated oracle.
inline std::vector<std::byte> make_payload(std::size_t n, std::uint8_t salt)
{
    std::vector<std::byte> out(n);
    for(std::size_t i = 0; i < n; ++i)
        out[i] = static_cast<std::byte>((i * 31u + salt * 7u + (i >> 8)) & 0xFFu);
    return out;
}

inline bool equal_bytes(std::span<const std::byte> a, std::span<const std::byte> b)
{
    return a.size() == b.size() && std::equal(a.begin(), a.end(), b.begin());
}

template<typename Pred>
inline void pump_until(::asio::io_context &io, Pred pred, ms timeout = ms{15000})
{
    auto bound = std::chrono::steady_clock::now() + timeout;
    while(!pred() && std::chrono::steady_clock::now() < bound)
    {
        io.poll();
        if(io.stopped())
            io.restart();
    }
}

}

#endif
