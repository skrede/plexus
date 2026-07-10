#include "plexus/asio/detail/interface_resolve.h"

#include "plexus/io/network_interface.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/ip/address_v4.hpp>

#include <string>
#include <vector>
#include <variant>
#include <optional>

namespace {

namespace pio     = plexus::io;
namespace pdetail = plexus::asio::detail;

std::optional<pdetail::interface_record> first_loopback()
{
    for(const auto &record : pdetail::enumerate_v4_interfaces())
        if(record.address.is_loopback())
            return record;
    return std::nullopt;
}

TEST_CASE("network_interface selector holds the four pure-data forms", "[network_interface][io]")
{
    REQUIRE(pio::network_interface::any().is_any());

    const auto by_name = pio::network_interface::by_name("eth0");
    REQUIRE_FALSE(by_name.is_any());
    REQUIRE(std::holds_alternative<pio::network_interface::name_t>(by_name.value()));
    REQUIRE(std::get<pio::network_interface::name_t>(by_name.value()).value == "eth0");

    const auto by_address = pio::network_interface::by_address("127.0.0.1");
    REQUIRE(std::get<pio::network_interface::address_t>(by_address.value()).value == "127.0.0.1");

    const auto by_index = pio::network_interface::by_index(7);
    REQUIRE(std::get<pio::network_interface::index_t>(by_index.value()).value == 7u);
}

TEST_CASE("network_interface by_address resolves verbatim to its egress", "[network_interface][io]")
{
    const auto resolved = pdetail::resolve_interface(pio::network_interface::by_address("127.0.0.1"));
    REQUIRE_FALSE(resolved.ec);
    REQUIRE(resolved.set_outbound);
    REQUIRE(resolved.egress == ::asio::ip::make_address_v4("127.0.0.1"));
}

TEST_CASE("network_interface any resolves to a concrete effective interface", "[network_interface][io]")
{
    if(pdetail::enumerate_v4_interfaces().empty())
    {
        SKIP("no v4 interfaces enumerable on this host");
    }
    const auto resolved = pdetail::resolve_interface(pio::network_interface::any());
    REQUIRE_FALSE(resolved.ec);
    REQUIRE_FALSE(resolved.set_outbound);
    REQUIRE_FALSE(resolved.effective_name.empty());
}

TEST_CASE("network_interface by_name and by_index resolve the loopback", "[network_interface][io]")
{
    const auto loopback = first_loopback();
    if(!loopback)
    {
        SKIP("no v4 loopback enumerable on this host");
    }

    const auto by_name = pdetail::resolve_interface(pio::network_interface::by_name(loopback->name));
    REQUIRE_FALSE(by_name.ec);
    REQUIRE(by_name.set_outbound);
    REQUIRE(by_name.egress.is_loopback());

    const auto by_index = pdetail::resolve_interface(pio::network_interface::by_index(loopback->index));
    REQUIRE_FALSE(by_index.ec);
    REQUIRE(by_index.egress.is_loopback());
}

TEST_CASE("network_interface bogus selector fails closed, not thrown", "[network_interface][io]")
{
    const auto resolved = pdetail::resolve_interface(pio::network_interface::by_name("nonexistent-nic-xyz"));
    REQUIRE(resolved.ec);
    REQUIRE_FALSE(resolved.set_outbound);
}

}
