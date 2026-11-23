#include "plexus/asio/detail/asio_unix_endpoint_parse.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <system_error>

namespace pasio = plexus::asio;

TEST_CASE("parse_unix fails closed on empty and oversized unix socket paths", "[integration][unix][afunix][security]")
{
    std::error_code ec;
    (void)pasio::detail::parse_unix("", ec);
    REQUIRE(ec);
    ec.clear();
    const std::string oversized(4096, 'a');
    (void)pasio::detail::parse_unix(oversized, ec);
    REQUIRE(ec);

    ec.clear();
    (void)pasio::detail::parse_unix("pxs", ec);
    REQUIRE_FALSE(ec);
}
