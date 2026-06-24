#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_ASIO_SERIAL_ENDPOINT_PARSE_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_ASIO_SERIAL_ENDPOINT_PARSE_H

#include <string>
#include <cstdint>
#include <charconv>
#include <system_error>

namespace plexus::asio::detail {

struct serial_endpoint
{
    std::string device;
    std::uint32_t baud{0};
};

// An empty/missing device, or a missing/unparseable/zero baud, sets ec and returns {} (fail
// closed) rather than opening a wrong device or a zero-baud line.
inline serial_endpoint parse_serial(const std::string &addr, std::error_code &ec)
{
    const auto at = addr.find('@');
    if(at == std::string::npos)
    {
        ec = std::make_error_code(std::errc::invalid_argument);
        return {};
    }
    const auto device   = addr.substr(0, at);
    const auto baud_str = addr.substr(at + 1);
    std::uint32_t baud  = 0;
    auto [ptr, e]       = std::from_chars(baud_str.data(), baud_str.data() + baud_str.size(), baud);
    if(device.empty() || baud_str.empty() || e != std::errc{} || ptr != baud_str.data() + baud_str.size() || baud == 0)
    {
        ec = std::make_error_code(std::errc::invalid_argument);
        return {};
    }
    return {device, baud};
}

}

#endif
