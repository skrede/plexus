#ifndef HPP_GUARD_PLEXUS_DETAIL_ADDRESS_PARSE_H
#define HPP_GUARD_PLEXUS_DETAIL_ADDRESS_PARSE_H

#include "plexus/io/endpoint.h"
#include "plexus/node_id.h"
#include "plexus/discovery/contact_card.h"

#include <string>
#include <vector>
#include <utility>
#include <charconv>
#include <optional>
#include <string_view>

namespace plexus::detail {

inline std::optional<std::uint16_t> port_of_value(std::string_view v)
{
    std::uint16_t port{};
    const char *first = v.data();
    const char *last = v.data() + v.size();
    const auto res = std::from_chars(first, last, port);
    if(res.ec != std::errc{} || res.ptr != last)
        return std::nullopt;
    return port;
}

inline std::optional<plexus::node_id>
card_node_id(const std::vector<std::pair<std::string, std::string>> &card)
{
    for(const auto &[k, v] : card)
        if(k == discovery::k_card_node_id_key)
            return discovery::detail::hex_decode(v);
    return std::nullopt;
}

// The host portion of a "host:port" address, with any trailing ":port" stripped.
// An IPv6-style address (multiple colons) is left verbatim — only a single
// trailing host:port pair is split.
inline std::string host_of(const std::string &address)
{
    const auto colon = address.rfind(':');
    if(colon == std::string::npos)
        return address;
    return address.substr(0, colon);
}

// The first "plexus/<scheme>/port" key in card order, resolved to {scheme, host:port}.
inline std::optional<io::endpoint>
first_port_endpoint(const std::vector<std::pair<std::string, std::string>> &card,
                    const std::string &host)
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
        const std::string_view scheme =
            key.substr(k_prefix.size(), key.size() - k_prefix.size() - k_suffix.size());
        if(const auto port = port_of_value(v))
            return io::endpoint{std::string{scheme}, host + ":" + std::to_string(*port)};
    }
    return std::nullopt;
}

// Parse the explicit port out of a "host:port" listen address (guarded — a missing
// or non-numeric port yields absence, the auto-assign-not-advertisable precondition).
inline std::optional<std::uint16_t> port_of(const std::string &address)
{
    const auto colon = address.rfind(':');
    if(colon == std::string::npos)
        return std::nullopt;
    return port_of_value(address.substr(colon + 1));
}

}

#endif
