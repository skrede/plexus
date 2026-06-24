// Live connectionless-UDP best_effort transport leg over a real loopback socket pair.
// Two udp_transports share one io_context: one listens, one dials; the handshake ARQ
// establishes the session, then a best_effort frame flows dialer -> acceptor and the
// accepting channel's on_data posts the IDENTICAL bytes. The two concept gates
// (byte_channel<udp_channel>, transport_backend<udp_transport, udp_policy>) are restated
// in the base TU so it is self-evidently the proof.
#pragma once

#include "plexus/asio/udp_channel.h"
#include "plexus/asio/udp_policy.h"
#include "plexus/asio/udp_server.h"
#include "plexus/asio/udp_transport.h"

#include "plexus/wire/udp_ack.h"
#include "plexus/wire/udp_envelope.h"

#include "plexus/io/byte_channel.h"
#include "plexus/io/fragmentation.h"
#include "plexus/io/transport_backend.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>
#include <asio/ip/udp.hpp>

#include <span>
#include <string>
#include <vector>
#include <chrono>
#include <memory>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace udp_transport_fixture {

namespace pasio = plexus::asio;
namespace pio   = plexus::io;
namespace wire  = plexus::wire;

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

} // namespace udp_transport_fixture
