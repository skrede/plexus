#include "plexus/expected.h"
#include "plexus/call_error.h"

#include "plexus/wire/rpc_status.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <system_error>

namespace {

using plexus::expected;
using plexus::unexpected;
using plexus::unexpect;
using plexus::call_errc;
using plexus::make_error_code;
using plexus::from_rpc_status;
using plexus::wire::rpc_status;

TEST_CASE("expected carries a value", "[vocabulary][expected]")
{
    expected<int, std::error_code> e{7};

    REQUIRE(static_cast<bool>(e));
    REQUIRE(e.has_value());
    REQUIRE(*e == 7);
    REQUIRE(e.value() == 7);
    REQUIRE(e.value_or(99) == 7);
}

TEST_CASE("expected carries an error via unexpected", "[vocabulary][expected]")
{
    expected<int, std::error_code> e{unexpected<std::error_code>(make_error_code(call_errc::timeout))};

    REQUIRE_FALSE(static_cast<bool>(e));
    REQUIRE_FALSE(e.has_value());
    REQUIRE(e.error() == call_errc::timeout);
    REQUIRE(e.value_or(99) == 99);
}

TEST_CASE("expected unexpect-tag construction", "[vocabulary][expected]")
{
    expected<int, std::error_code> e{unexpect, make_error_code(call_errc::no_provider)};

    REQUIRE_FALSE(e);
    REQUIRE(e.error() == call_errc::no_provider);
}

TEST_CASE("expected with a move-only value round-trips through value()", "[vocabulary][expected]")
{
    expected<std::string, std::error_code> e{std::string("payload")};

    REQUIRE(e.has_value());
    REQUIRE(e->size() == 7);
    REQUIRE(std::move(e).value() == "payload");
}

TEST_CASE("expected<void> default-constructs to a success state", "[vocabulary][expected]")
{
    expected<void, std::error_code> e;

    REQUIRE(static_cast<bool>(e));
    REQUIRE(e.has_value());
}

TEST_CASE("expected<void> carries an error via unexpected", "[vocabulary][expected]")
{
    expected<void, std::error_code> e{
        unexpected<std::error_code>(make_error_code(call_errc::deserialize_failed))};

    REQUIRE_FALSE(static_cast<bool>(e));
    REQUIRE_FALSE(e.has_value());
    REQUIRE(e.error() == call_errc::deserialize_failed);
}

TEST_CASE("expected<void> unexpect-tag construction", "[vocabulary][expected]")
{
    expected<void, std::error_code> e{unexpect, make_error_code(call_errc::error)};

    REQUIRE_FALSE(e);
    REQUIRE(e.error() == call_errc::error);
}

TEST_CASE("call_errc integrates with std::error_code", "[vocabulary][call_errc]")
{
    std::error_code ec = call_errc::no_provider;

    REQUIRE(ec);
    REQUIRE(ec == call_errc::no_provider);
    REQUIRE(ec != call_errc::timeout);
    REQUIRE(std::string(ec.category().name()) == "plexus.call");
}

TEST_CASE("call_errc category name is stable across codes", "[vocabulary][call_errc]")
{
    const std::error_code a = make_error_code(call_errc::error);
    const std::error_code b = make_error_code(call_errc::no_handler);

    REQUIRE(&a.category() == &b.category());
    REQUIRE(a.category().name() == b.category().name());
}

TEST_CASE("distinct call_errc map to distinct codes", "[vocabulary][call_errc]")
{
    REQUIRE(make_error_code(call_errc::timeout) != make_error_code(call_errc::cancelled));
    REQUIRE(make_error_code(call_errc::no_provider) != make_error_code(call_errc::error));
}

TEST_CASE("from_rpc_status maps every failure status", "[vocabulary][call_errc]")
{
    REQUIRE(from_rpc_status(rpc_status::error)               == call_errc::error);
    REQUIRE(from_rpc_status(rpc_status::timeout)             == call_errc::timeout);
    REQUIRE(from_rpc_status(rpc_status::cancelled)           == call_errc::cancelled);
    REQUIRE(from_rpc_status(rpc_status::no_handler)          == call_errc::no_handler);
    REQUIRE(from_rpc_status(rpc_status::deserialize_failed)  == call_errc::deserialize_failed);
    REQUIRE(from_rpc_status(rpc_status::topic_not_found)     == call_errc::topic_not_found);
    REQUIRE(from_rpc_status(rpc_status::peer_disconnected)   == call_errc::peer_disconnected);
    REQUIRE(from_rpc_status(rpc_status::rpc_response_orphan) == call_errc::rpc_response_orphan);
}

}
