// The reliable-datagram opt-in engaged through the SCHEME (the engine-reachable dial
// path): a dial("udpr://...") mints a reliable-datagram channel whose single send() verb
// drives the in-order ARQ; a dial("udp://...") mints a best_effort channel (fire-and-
// forget). The mode is declared in the handshake so the acceptor is SYMMETRIC — both ends
// agree on the class. The mux-level route flip ("udpr" -> the UDP+ARQ member, never TCP)
// is pinned in test_udp_transport.cpp; this TU pins the transport+channel MODE mechanics
// underneath it.
#pragma once

#include "plexus/asio/udp_channel.h"
#include "plexus/asio/udp_server.h"
#include "plexus/asio/udp_transport.h"

#include "plexus/wire/udp_ack.h"
#include "plexus/wire/udp_envelope.h"

#include "plexus/datagram/detail/udp_handshake_frame.h"

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

namespace udp_reliable_datagram_fixture {

namespace pasio = plexus::asio;
namespace wire  = plexus::wire;
namespace pio   = plexus::io;

using ms = std::chrono::milliseconds;

constexpr pasio::udp_transport::arq_type::schedule fast_hs{ms{20}, ms{40}, ms{80}};

inline plexus::datagram::detail::udp_arq_config fast_arq()
{
    return plexus::datagram::detail::udp_arq_config{.window = 64, .initial_rto = ms{20}, .min_rto = ms{10}, .max_rto = ms{80}, .max_retransmit = 12};
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

enum class action
{
    pass,
    drop
};

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

} // namespace udp_reliable_datagram_fixture
