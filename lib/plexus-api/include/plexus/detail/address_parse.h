#ifndef HPP_GUARD_PLEXUS_DETAIL_ADDRESS_PARSE_H
#define HPP_GUARD_PLEXUS_DETAIL_ADDRESS_PARSE_H

#include "plexus/discovery/contact_card.h"

#include "plexus/io/endpoint.h"

#include <string>
#include <vector>
#include <utility>
#include <optional>
#include <string_view>

namespace plexus::detail {

// An IPv6-style address (multiple colons) is left verbatim — only a single trailing host:port
// pair is split.
inline std::string host_of(const std::string &address)
{
    const auto colon = address.rfind(':');
    if(colon == std::string::npos)
        return address;
    return address.substr(0, colon);
}

inline std::optional<io::endpoint> first_port_endpoint(const std::vector<std::pair<std::string, std::string>> &card, const std::string &host)
{
    constexpr std::string_view k_prefix = "plexus/";
    constexpr std::string_view k_suffix = "/port";
    for(const auto &[k, v] : card)
    {
        std::string_view key{k};
        if(key.size() <= k_prefix.size() + k_suffix.size())
            continue;
        if(key.substr(0, k_prefix.size()) != k_prefix)
            continue;
        if(key.substr(key.size() - k_suffix.size()) != k_suffix)
            continue;
        const std::string_view scheme = key.substr(k_prefix.size(), key.size() - k_prefix.size() - k_suffix.size());
        if(const auto port = discovery::detail::parse_port(v))
            return io::endpoint{std::string{scheme}, host + ":" + std::to_string(*port)};
    }
    return std::nullopt;
}

// A missing or non-numeric port yields absence (fail-closed): an auto-assigned port is not
// advertisable.
inline std::optional<std::uint16_t> port_of(const std::string &address)
{
    const auto colon = address.rfind(':');
    if(colon == std::string::npos)
        return std::nullopt;
    return discovery::detail::parse_port(std::string_view{address}.substr(colon + 1));
}

}

#endif
