#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_ASIO_ENDPOINT_PARSE_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_ASIO_ENDPOINT_PARSE_H

#include <asio/ip/tcp.hpp>
#include <asio/ip/address.hpp>

#include <string>
#include <cstdint>
#include <system_error>

namespace plexus::asio::detail {

// Parse a "host:port" address into a tcp::endpoint. A missing colon or an
// unparseable host sets ec — the dial/listen path then fails closed rather than
// crashing on a malformed address. Shared by the listener (bind) and the
// transport (dial) so the split lives in one place.
inline ::asio::ip::tcp::endpoint parse(const std::string &addr, std::error_code &ec)
{
    auto colon = addr.rfind(':');
    if(colon == std::string::npos)
    {
        ec = std::make_error_code(std::errc::invalid_argument);
        return {};
    }
    auto host = addr.substr(0, colon);
    auto port = static_cast<uint16_t>(std::stoul(addr.substr(colon + 1)));
    return {::asio::ip::make_address(host, ec), port};
}

}

#endif
