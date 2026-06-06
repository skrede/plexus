#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_ASIO_UDP_ENDPOINT_PARSE_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_ASIO_UDP_ENDPOINT_PARSE_H

#include <asio/ip/udp.hpp>
#include <asio/ip/address.hpp>

#include <string>
#include <cstdint>
#include <system_error>

namespace plexus::asio::detail {

// Parse a "host:port" address into a udp::endpoint. A missing colon or an
// unparseable host sets ec — the dial/bind path then fails closed rather than
// crashing on a malformed address. A protocol-type swap of the tcp parse (udp vs
// tcp is the only change); shared by the transport bind (listen) and dial so the
// split lives in one place.
inline ::asio::ip::udp::endpoint parse_udp(const std::string &addr, std::error_code &ec)
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
