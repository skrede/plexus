#include "plexus/asio/detail/accept_retry.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/error.hpp>
#include <asio/io_context.hpp>

#include <chrono>
#include <vector>
#include <cstdint>
#include <system_error>

using plexus::asio::detail::accept_backoff;
using plexus::asio::detail::accept_verdict;
using plexus::asio::detail::classify_accept_error;
using namespace std::chrono_literals;

namespace {

accept_verdict of(std::errc e)
{
    return classify_accept_error(std::make_error_code(e));
}

accept_verdict of(::asio::error::basic_errors e)
{
    return classify_accept_error(::asio::error::make_error_code(e));
}

}

TEST_CASE("accept_retry: the fatal set stops, resource exhaustion backs off, and every other error re-arms", "[asio][accept]")
{
    CHECK(of(::asio::error::operation_aborted) == accept_verdict::stop);
    CHECK(of(std::errc::operation_canceled) == accept_verdict::stop);
    CHECK(of(::asio::error::bad_descriptor) == accept_verdict::stop);
    CHECK(of(std::errc::bad_file_descriptor) == accept_verdict::stop);
    CHECK(of(::asio::error::invalid_argument) == accept_verdict::stop);
    CHECK(of(std::errc::invalid_argument) == accept_verdict::stop);
    CHECK(of(::asio::error::not_socket) == accept_verdict::stop);
    CHECK(of(std::errc::not_a_socket) == accept_verdict::stop);

    CHECK(of(::asio::error::no_descriptors) == accept_verdict::backoff);
    CHECK(of(std::errc::too_many_files_open) == accept_verdict::backoff);
    CHECK(of(std::errc::too_many_files_open_in_system) == accept_verdict::backoff);
    CHECK(of(::asio::error::no_buffer_space) == accept_verdict::backoff);
    CHECK(of(std::errc::no_buffer_space) == accept_verdict::backoff);
    CHECK(of(::asio::error::no_memory) == accept_verdict::backoff);
    CHECK(of(std::errc::not_enough_memory) == accept_verdict::backoff);

    CHECK(of(::asio::error::connection_aborted) == accept_verdict::rearm);
    CHECK(of(std::errc::connection_aborted) == accept_verdict::rearm);
    CHECK(of(std::errc::protocol_error) == accept_verdict::rearm);
    CHECK(of(::asio::error::network_down) == accept_verdict::rearm);
    CHECK(of(std::errc::network_down) == accept_verdict::rearm);
    CHECK(of(::asio::error::host_unreachable) == accept_verdict::rearm);
    CHECK(of(std::errc::host_unreachable) == accept_verdict::rearm);
    CHECK(of(::asio::error::connection_reset) == accept_verdict::rearm);
    CHECK(of(std::errc::connection_reset) == accept_verdict::rearm);
}

TEST_CASE("accept_retry: the backoff doubles from 5 ms to a 1 s cap and resets after a success", "[asio][accept]")
{
    ::asio::io_context io;
    accept_backoff backoff{io};
    std::vector<std::chrono::milliseconds> seen;
    bool fired = false;
    for(std::int32_t i = 0; i < 12; ++i)
    {
        seen.push_back(backoff.delay());
        backoff.wait([&] { fired = true; });
        REQUIRE(backoff.pending() == 1);
        backoff.cancel();
        io.restart();
        io.poll();
        REQUIRE(backoff.pending() == 0);
    }
    CHECK_FALSE(fired);
    CHECK(seen == std::vector<std::chrono::milliseconds>{5ms, 10ms, 20ms, 40ms, 80ms, 160ms, 320ms, 640ms, 1000ms, 1000ms, 1000ms, 1000ms});

    backoff.reset();
    REQUIRE(backoff.delay() == 5ms);
    backoff.wait([&] { fired = true; });
    io.restart();
    io.run();
    CHECK(fired);
    CHECK(backoff.pending() == 0);
    CHECK(backoff.delay() == 10ms);
}
