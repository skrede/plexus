#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_INTERFACE_RESOLVE_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_INTERFACE_RESOLVE_H

#include "plexus/io/network_interface.h"

#include <asio/ip/address_v4.hpp>

#include <vector>
#include <string>
#include <variant>
#include <cstdint>
#include <system_error>

#if defined(_WIN32)
    #include <winsock2.h>
    #include <ws2tcpip.h>
    #include <iphlpapi.h>
#elif defined(__unix__) || defined(__APPLE__)
    #include <ifaddrs.h>
    #include <net/if.h>
    #include <netinet/in.h>
    #include <arpa/inet.h>
#endif

namespace plexus::asio::detail {

struct interface_record
{
    std::string name;
    ::asio::ip::address_v4 address;
    std::uint32_t index;
};

// The resolved egress. set_outbound is false for any() (the OS chooses; IP_MULTICAST_IF is left
// unset). effective_name is a diagnosability hint (D-09): for any() it is the first enumerated
// non-loopback interface (or a route match), which on a multi-NIC host need NOT equal the kernel's
// true per-destination egress — it is reported, never asserted as the true egress.
struct interface_resolution
{
    ::asio::ip::address_v4 egress;
    std::string effective_name;
    bool set_outbound;
    std::error_code ec;
};

#if defined(_WIN32)
inline std::vector<interface_record> enumerate_v4_interfaces()
{
    std::vector<interface_record> out;
    ULONG size = 0;
    if(::GetAdaptersAddresses(AF_INET, 0, nullptr, nullptr, &size) != ERROR_BUFFER_OVERFLOW)
        return out;
    std::vector<unsigned char> buffer(size);
    auto *adapters = reinterpret_cast<IP_ADAPTER_ADDRESSES *>(buffer.data());
    if(::GetAdaptersAddresses(AF_INET, 0, nullptr, adapters, &size) != NO_ERROR)
        return out;
    for(auto *a = adapters; a != nullptr; a = a->Next)
        for(auto *u = a->FirstUnicastAddress; u != nullptr; u = u->Next)
        {
            const auto *sin = reinterpret_cast<const sockaddr_in *>(u->Address.lpSockaddr);
            out.push_back({std::string(a->AdapterName), ::asio::ip::make_address_v4(ntohl(sin->sin_addr.s_addr)), a->IfIndex});
        }
    return out;
}
#elif defined(__unix__) || defined(__APPLE__)
inline std::vector<interface_record> enumerate_v4_interfaces()
{
    std::vector<interface_record> out;
    ifaddrs *head = nullptr;
    if(::getifaddrs(&head) != 0)
        return out;
    for(ifaddrs *it = head; it != nullptr; it = it->ifa_next)
    {
        if(it->ifa_addr == nullptr || it->ifa_addr->sa_family != AF_INET)
            continue;
        const auto *sin = reinterpret_cast<const sockaddr_in *>(it->ifa_addr);
        out.push_back({it->ifa_name, ::asio::ip::make_address_v4(ntohl(sin->sin_addr.s_addr)), ::if_nametoindex(it->ifa_name)});
    }
    ::freeifaddrs(head);
    return out;
}
#endif

inline const interface_record *find_by_name(const std::vector<interface_record> &records, const std::string &name)
{
    for(const auto &r : records)
        if(r.name == name)
            return &r;
    return nullptr;
}

inline const interface_record *find_by_index(const std::vector<interface_record> &records, std::uint32_t index)
{
    for(const auto &r : records)
        if(r.index == index)
            return &r;
    return nullptr;
}

inline const interface_record *find_by_address(const std::vector<interface_record> &records, const ::asio::ip::address_v4 &address)
{
    for(const auto &r : records)
        if(r.address == address)
            return &r;
    return nullptr;
}

inline interface_resolution resolve_any(const std::vector<interface_record> &records)
{
    interface_resolution result{::asio::ip::address_v4::any(), std::string{}, false, std::error_code{}};
    for(const auto &record : records)
        if(!record.address.is_loopback())
        {
            result.effective_name = record.name;
            return result;
        }
    if(!records.empty())
        result.effective_name = records.front().name;
    return result;
}

inline interface_resolution resolve_matched(const interface_record *record)
{
    if(record == nullptr)
        return {::asio::ip::address_v4::any(), std::string{}, false, std::make_error_code(std::errc::no_such_device)};
    return {record->address, record->name, true, std::error_code{}};
}

inline interface_resolution resolve_address(const std::vector<interface_record> &records, const std::string &text)
{
    std::error_code parse_ec;
    const auto address = ::asio::ip::make_address_v4(text, parse_ec);
    if(parse_ec)
        return {::asio::ip::address_v4::any(), std::string{}, false, std::make_error_code(std::errc::invalid_argument)};
    const interface_record *record = find_by_address(records, address);
    return {address, record != nullptr ? record->name : text, true, std::error_code{}};
}

// Resolve the selector to an egress once, eagerly (no io loop). A miss is a fail-closed value, never
// a throw: the caller routes the error_code to its fail-closed on_error path (recv loop never armed).
inline interface_resolution resolve_interface(const plexus::io::network_interface &iface)
{
    const auto records   = enumerate_v4_interfaces();
    const auto &selector = iface.value();
    if(std::holds_alternative<plexus::io::network_interface::any_t>(selector))
        return resolve_any(records);
    if(const auto *by_name = std::get_if<plexus::io::network_interface::name_t>(&selector))
        return resolve_matched(find_by_name(records, by_name->value));
    if(const auto *by_address = std::get_if<plexus::io::network_interface::address_t>(&selector))
        return resolve_address(records, by_address->value);
    const auto &by_index = std::get<plexus::io::network_interface::index_t>(selector);
    return resolve_matched(find_by_index(records, by_index.value));
}

}

#endif
