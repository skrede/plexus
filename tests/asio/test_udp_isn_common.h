// The randomized per-session initial sequence number (RFC 6528 lineage): the udpr
// reliable-data path no longer starts both ends at a predictable 0. The dialer picks a
// random per-session ISN, advertises it in the UDP handshake inner frame, the acceptor
// echoes it (symmetric, exactly like the channel-mode echo), and both ends feed the
// resolved ISN into the reorder-buffer / ARQ ctor seam. An off-path attacker can then no
// longer inject an accepted [kind=reliable-data][seq=0..] segment on the PLAINTEXT udpr
// path (under AEAD the forged segment also dies at the tag check, so this is the
// plaintext-path defense).
#pragma once

#include "plexus/asio/udp_channel.h"
#include "plexus/asio/udp_server.h"
#include "plexus/asio/udp_transport.h"

#include "plexus/wire/udp_envelope.h"

#include "plexus/datagram/detail/udp_handshake_frame.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/buffer.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/udp.hpp>

#include <span>
#include <array>
#include <chrono>
#include <memory>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <iterator>
#include <algorithm>

namespace udp_isn_fixture {

namespace pasio = plexus::asio;
namespace wire  = plexus::wire;
namespace pio   = plexus::io;

using ms = std::chrono::milliseconds;

constexpr pasio::udp_transport::arq_type::schedule fast_hs{ms{20}, ms{40}, ms{80}};

inline plexus::datagram::detail::udp_arq_config fast_arq()
{
    return plexus::datagram::detail::udp_arq_config{.window         = 64,
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
void pump_until(::asio::io_context &io, Pred pred, ms timeout = ms{6000})
{
    auto bound = std::chrono::steady_clock::now() + timeout;
    while(!pred() && std::chrono::steady_clock::now() < bound)
    {
        io.poll();
        if(io.stopped())
            io.restart();
    }
}

} // namespace udp_isn_fixture
