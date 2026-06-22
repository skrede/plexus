#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_ASIO_SERIAL_ENDPOINT_PARSE_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_ASIO_SERIAL_ENDPOINT_PARSE_H

#include <string>
#include <cstdint>
#include <charconv>
#include <system_error>

namespace plexus::asio::detail {

// The parsed "/dev/ttyXXX@baud" address: the device path the serial_port opens by name and the
// authoritative line baud. The path-after-@-strip is what asio's serial_port(device) takes; the
// baud rides into serial_port_base::baud_rate at open. flow-control / character-size are NOT in
// the address — they are transport-ctor defaults (the @baud is the only per-endpoint line knob).
struct serial_endpoint
{
    std::string   device;
    std::uint32_t baud{0};
};

// Parse a "/dev/ttyXXX@baud" address fail-closed. Unlike the TCP host:port split there is no
// host — the part before '@' IS the device path. A missing/empty path, a missing/empty baud, an
// unparseable or zero baud all set ec and return {} — the dial/listen path then fails closed
// rather than opening a wrong/empty device or a zero-baud line. The baud is parsed with
// std::from_chars (no-throw), exactly like the TCP port parse. Shared by listen and dial so the
// validation lives in one place.
inline serial_endpoint parse_serial(const std::string &addr, std::error_code &ec)
{
    const auto at = addr.find('@');
    if(at == std::string::npos)
    {
        ec = std::make_error_code(std::errc::invalid_argument);
        return {};
    }
    const auto device   = addr.substr(0, at);
    const auto baud_str  = addr.substr(at + 1);
    std::uint32_t baud   = 0;
    auto [ptr, e]        = std::from_chars(baud_str.data(), baud_str.data() + baud_str.size(), baud);
    if(device.empty() || baud_str.empty() || e != std::errc{}
       || ptr != baud_str.data() + baud_str.size() || baud == 0)
    {
        ec = std::make_error_code(std::errc::invalid_argument);
        return {};
    }
    return {device, baud};
}

}

#endif
