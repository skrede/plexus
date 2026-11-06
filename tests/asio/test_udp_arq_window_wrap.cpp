#include "plexus/asio/asio_timer.h"

#include "plexus/datagram/detail/udp_reliable_arq.h"

#include "plexus/wire/udp_ack.h"

#include "plexus/testing/pump.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <map>
#include <span>
#include <array>
#include <chrono>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>

namespace {

namespace pasio = plexus::asio;
namespace wire  = plexus::wire;

using engine = plexus::datagram::detail::udp_reliable_arq<::asio::io_context &, pasio::asio_timer>;
using config = plexus::datagram::detail::udp_arq_config;
using bytes  = std::vector<std::byte>;

bytes bytes_of(const std::string &s)
{
    bytes out(s.size());
    for(std::size_t i = 0; i < s.size(); ++i)
        out[i] = static_cast<std::byte>(s[i]);
    return out;
}

struct capture
{
    std::map<std::uint16_t, bytes> first;
    std::map<std::uint16_t, bytes> resent;
    std::uint16_t last_seq{0};
    bytes last_bytes;

    void record(std::uint16_t seq, std::span<const std::byte> sent)
    {
        bytes copy(sent.begin(), sent.end());
        if(first.contains(seq))
            resent.emplace(seq, copy);
        else
            first.emplace(seq, copy);
        last_seq   = seq;
        last_bytes = std::move(copy);
    }
};

}

TEST_CASE("udp reliable_arq: a non-power-of-two window keeps every in-flight segment distinct across the sequence wrap", "[udp][reliable_arq]")
{
    ::asio::io_context io;
    engine arq{io, config{.window = 3, .initial_rto = std::chrono::milliseconds{20}, .min_rto = std::chrono::milliseconds{20}}, 65534};
    capture c;
    arq.on_transmit([&c](std::uint16_t seq, std::span<const std::byte> sent, bool) { c.record(seq, sent); });

    for(const std::string &payload : std::array<std::string, 3>{"before", "at", "after"})
        REQUIRE(arq.submit(bytes_of(payload)) == engine::submit_result::admitted);
    REQUIRE(c.first.size() == 3);

    plexus::testing::pump_until(io, [&c] { return c.resent.size() == 3; }, std::chrono::milliseconds{2000});
    arq.cancel();
    io.poll();

    REQUIRE(c.resent.size() == 3);
    for(const auto &[seq, sent] : c.resent)
        REQUIRE(sent == c.first.at(seq));
}

TEST_CASE("udp reliable_arq: a non-power-of-two window advances across the wrap as acks arrive", "[udp][reliable_arq]")
{
    ::asio::io_context io;
    engine arq{io, config{.window = 5}, 65534};
    capture c;
    arq.on_transmit([&c](std::uint16_t seq, std::span<const std::byte> sent, bool) { c.record(seq, sent); });

    for(std::uint32_t i = 0; i < 5; ++i)
        REQUIRE(arq.submit(bytes_of("p" + std::to_string(i))) == engine::submit_result::admitted);
    REQUIRE(arq.in_flight() == 5);
    REQUIRE_FALSE(arq.window_has_room());

    for(std::uint32_t step = 0; step < 5; ++step)
    {
        wire::udp_ack ack{};
        ack.cumulative = static_cast<std::uint16_t>(65534 + step);
        arq.on_ack(ack);
        REQUIRE(arq.in_flight() == 4);
        REQUIRE(arq.window_has_room());

        const bytes payload = bytes_of("p" + std::to_string(5 + step));
        REQUIRE(arq.submit(payload) == engine::submit_result::admitted);
        REQUIRE(c.last_seq == static_cast<std::uint16_t>(65534 + 5 + step));
        REQUIRE(c.last_bytes == payload);
        REQUIRE(arq.in_flight() == 5);
        REQUIRE_FALSE(arq.window_has_room());
    }

    arq.cancel();
    io.poll();
}
