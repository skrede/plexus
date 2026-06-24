#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_ASIO_ENDPOINT_PARSE_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_ASIO_ENDPOINT_PARSE_H

#include <asio/ip/tcp.hpp>
#include <asio/ip/address.hpp>

#include <string>
#include <charconv>
#include <cstdint>
#include <system_error>

namespace plexus::asio::detail {

// A missing colon, unparseable host, or out-of-range port sets ec and returns {} (fail closed).
inline ::asio::ip::tcp::endpoint parse(const std::string &addr, std::error_code &ec)
{
    auto colon = addr.rfind(':');
    if(colon == std::string::npos)
    {
        ec = std::make_error_code(std::errc::invalid_argument);
        return {};
    }
    auto host              = addr.substr(0, colon);
    auto port_str          = addr.substr(colon + 1);
    unsigned long port_val = 0;
    auto [ptr, e]          = std::from_chars(port_str.data(), port_str.data() + port_str.size(), port_val);
    if(e != std::errc{} || ptr != port_str.data() + port_str.size() || port_val > 65535u)
    {
        ec = std::make_error_code(std::errc::invalid_argument);
        return {};
    }
    return {::asio::ip::make_address(host, ec), static_cast<std::uint16_t>(port_val)};
}

}

#endif
