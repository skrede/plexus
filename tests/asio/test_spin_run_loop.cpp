// The opt-in spin-then-park driver over a user-owned io_context. Proves, over the
// REAL udp receive path (not a mock):
//   * a full handshake + best_effort round-trip completes when the io_context is
//     driven ONLY through spin_run_loop::poll_spin() -- the adapter drives the live
//     reactor end-to-end, it is not a side channel.
//   * default-equivalence: spin_budget 0 (drain-then-park) and spin_budget 256 both
//     deliver the identical bytes -- the spin is a latency policy, never a
//     correctness change.
//   * run() exits cleanly when the io_context is stopped (the park is breakable).
// The round-trip loops in-body and the ctest invocation is re-run across >=3 process
// runs for cross-process reproducibility (a live-socket claim is never made from one
// run).

#include "plexus/asio/spin_run_loop.h"
#include "plexus/asio/udp_channel.h"
#include "plexus/asio/udp_transport.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>
#include <asio/post.hpp>

#include <span>
#include <string>
#include <vector>
#include <chrono>
#include <memory>
#include <cstddef>
#include <optional>

namespace pasio = plexus::asio;

namespace {

std::vector<std::byte> bytes_of(const std::string &s)
{
    std::vector<std::byte> out(s.size());
    for(std::size_t i = 0; i < s.size(); ++i)
        out[i] = static_cast<std::byte>(s[i]);
    return out;
}

std::string str_of(std::span<const std::byte> b)
{
    std::string s(b.size(), '\0');
    for(std::size_t i = 0; i < b.size(); ++i)
        s[i] = static_cast<char>(std::to_integer<unsigned char>(b[i]));
    return s;
}

// A real loopback pair on one io_context, pumped exclusively through a spin_run_loop
// at the requested budget so the driver -- not a raw poll loop -- carries every leg.
struct spin_loopback
{
    ::asio::io_context io;
    pasio::spin_run_loop loop;
    pasio::udp_transport server{io};
    pasio::udp_transport client{io};

    std::unique_ptr<pasio::udp_channel> accepted;
    std::unique_ptr<pasio::udp_channel> dialed;
    std::vector<std::string> received;

    explicit spin_loopback(std::uint32_t spin_budget)
            : loop(io, spin_budget)
    {
        server.on_accepted(
                [this](std::unique_ptr<pasio::udp_channel> ch)
                {
                    accepted = std::move(ch);
                    accepted->on_data([this](std::span<const std::byte> b) { received.push_back(str_of(b)); });
                });
        server.listen({"udp", "127.0.0.1:0"});
        pump_until([this] { return server.port() != 0; });

        client.on_dialed([this](std::unique_ptr<pasio::udp_channel> ch, const plexus::io::endpoint &) { dialed = std::move(ch); });
        client.dial({"udp", "127.0.0.1:" + std::to_string(server.port())});
        pump_until([this] { return dialed != nullptr && accepted != nullptr; });
    }

    template<typename Pred>
    void pump_until(Pred pred)
    {
        auto bound = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while(!pred() && std::chrono::steady_clock::now() < bound)
            loop.poll_spin();
    }
};

void round_trips_at(std::uint32_t spin_budget)
{
    constexpr int k_iterations = 50;
    int proven                 = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        spin_loopback h{spin_budget};
        REQUIRE(h.dialed != nullptr);
        REQUIRE(h.accepted != nullptr);

        const std::string payload = "spin-" + std::to_string(iter);
        auto frame                = bytes_of(payload);
        h.dialed->send(frame);

        h.pump_until([&] { return !h.received.empty(); });
        REQUIRE(h.received.size() == 1);
        REQUIRE(h.received.front() == payload);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

}

TEST_CASE("spin_run_loop drives a live udp round-trip end-to-end at the default budget", "[asio][spin]")
{
    round_trips_at(pasio::spin_run_loop::default_spin_budget);
}

TEST_CASE("spin_run_loop budget 0 (drain-then-park) delivers identically -- the spin is a latency "
          "policy",
          "[asio][spin]")
{
    round_trips_at(0);
}

TEST_CASE("spin_run_loop::run() exits when the io_context is stopped", "[asio][spin]")
{
    ::asio::io_context io;
    pasio::spin_run_loop loop{io};

    bool ran = false;
    ::asio::post(io,
                 [&]
                 {
                     ran = true;
                     io.stop();
                 });

    loop.run(); // must return: the posted handler stops the context, breaking the park
    REQUIRE(ran);
    REQUIRE(io.stopped());
}
