#include "test_outofbox_message_ceiling_common.h"

#include "plexus/testing/platform.h"

#include <catch2/catch_test_macros.hpp>

using namespace outofbox_ceiling_fixture;

TEST_CASE("outofbox: an 8 MiB message round-trips over TCP at shipped defaults, looped", "[outofbox][envelope8]")
{
    const auto body  = ramp_payload(k_shipped_ceiling);
    const auto frame = ceiling_frame(body);

    constexpr int iterations = 2;
    int proven               = 0;
    for(int iter = 0; iter < iterations; ++iter)
    {
        ::asio::io_context io;
        pasio::asio_transport server{io}; // NO size/back-pressure knobs — full defaults
        pasio::asio_transport client{io};

        std::unique_ptr<pasio::asio_channel> accepted, dialed;
        std::vector<std::byte> got;
        server.on_accepted(
                [&](std::unique_ptr<pasio::asio_channel> ch)
                {
                    accepted = std::move(ch);
                    accepted->on_data([&](std::span<const std::byte> d) { got.assign(d.begin(), d.end()); });
                });
        client.on_dialed([&](std::unique_ptr<pasio::asio_channel> ch, const pio::endpoint &) { dialed = std::move(ch); });

        server.listen({"tcp", "127.0.0.1:0"});
        client.dial({"tcp", "127.0.0.1:" + std::to_string(server.port())});
        pump_until(io, [&] { return accepted && dialed; }, ms{8000});
        REQUIRE(accepted != nullptr);
        REQUIRE(dialed != nullptr);

        dialed->send(std::span<const std::byte>{frame});
        pump_until(io, [&] { return got.size() == frame.size(); });
        REQUIRE(got.size() == frame.size());
        REQUIRE(equal_bytes(got, frame)); // byte-equal at the shipped ceiling, default caps
        ++proven;
    }
    REQUIRE(proven == iterations);
}

TEST_CASE("outofbox: an 8 MiB message round-trips over AF_UNIX at shipped defaults, looped", "[outofbox][envelope8]")
{
    const auto body  = ramp_payload(k_shipped_ceiling);
    const auto frame = ceiling_frame(body);

    constexpr int iterations = 2;
    int proven               = 0;
    for(int iter = 0; iter < iterations; ++iter)
    {
        const std::filesystem::path dir  = plexus::testing::make_temp_dir("pxo-");
        const std::string           path = (dir / "s").string();

        ::asio::io_context io;
        pasio::unix_transport server{io}; // full defaults
        pasio::unix_transport client{io};

        std::unique_ptr<pasio::unix_channel> accepted, dialed;
        std::vector<std::byte> got;
        server.on_accepted(
                [&](std::unique_ptr<pasio::unix_channel> ch)
                {
                    accepted = std::move(ch);
                    accepted->on_data([&](std::span<const std::byte> d) { got.assign(d.begin(), d.end()); });
                });
        client.on_dialed([&](std::unique_ptr<pasio::unix_channel> ch, const pio::endpoint &) { dialed = std::move(ch); });

        server.listen({"unix", path});
        client.dial({"unix", path});
        pump_until(io, [&] { return accepted && dialed; }, ms{8000});
        REQUIRE(accepted != nullptr);
        REQUIRE(dialed != nullptr);

        dialed->send(std::span<const std::byte>{frame});
        pump_until(io, [&] { return got.size() == frame.size(); });
        REQUIRE(got.size() == frame.size());
        REQUIRE(equal_bytes(got, frame));
        ++proven;

        plexus::testing::remove_socket_path(path);
        std::filesystem::remove(dir);
    }
    REQUIRE(proven == iterations);
}
