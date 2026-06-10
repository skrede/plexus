#include "plexus/mdnspp/mdnspp_discovery.h"

#include <mdnspp/service_info.h>
#include <mdnspp/query_options.h>
#include <mdnspp/service_options.h>
#include <mdnspp/resolved_service.h>
#include <mdnspp/basic_service_server.h>
#include <mdnspp/basic_service_discovery.h>
#include <mdnspp/asio/asio_policy.h>

#include <string>
#include <vector>
#include <utility>

namespace plexus::mdnspp {

// The mdnspp peers are held here so the C++23 mdnspp templates never reach the
// adapter's public header. The server announces a local service; the discovery
// peer browses and aggregates resolved services. Both bind the caller's
// io_context (the shared executor).
struct mdnspp_discovery::impl
{
    explicit impl(::asio::io_context &io)
        : context(io)
    {
    }

    ::asio::io_context &context;
    std::unique_ptr<::mdnspp::basic_service_server<::mdnspp::asio_policy>> server;
    std::unique_ptr<::mdnspp::basic_service_discovery<::mdnspp::asio_policy>> browser;
};

namespace {

// Reconstruct the plexus tcp endpoint from an aggregated mDNS service: the
// first IPv4 address joined with the SRV port. Empty address when the service
// resolved without an A record (a partial service is still surfaced by name).
std::string tcp_address(const ::mdnspp::resolved_service &svc)
{
    if(svc.ipv4_addresses.empty())
        return {};
    return svc.ipv4_addresses.front() + ":" + std::to_string(svc.port);
}

// The contact card maps onto the RFC 6763 TXT record key/value entries: a faithful
// pass-through of whatever the card assembler produced (the assembler is the sole
// authority on what a card may contain, so the backend never injects or filters keys).
std::vector<::mdnspp::service_txt> to_txt_records(
    const std::vector<std::pair<std::string, std::string>> &metadata)
{
    std::vector<::mdnspp::service_txt> records;
    records.reserve(metadata.size());
    for(const auto &[key, value] : metadata)
        records.push_back(::mdnspp::service_txt{key, value});
    return records;
}

std::vector<std::pair<std::string, std::string>> from_txt_entries(
    const std::vector<::mdnspp::service_txt> &entries)
{
    std::vector<std::pair<std::string, std::string>> metadata;
    metadata.reserve(entries.size());
    for(const auto &entry : entries)
        metadata.emplace_back(entry.key, entry.value.value_or(std::string{}));
    return metadata;
}

}

mdnspp_discovery::mdnspp_discovery(::asio::io_context &io, std::string service_type)
    : m_io(io)
    , m_service_type(std::move(service_type))
    , m_impl(std::make_unique<impl>(io))
{
}

mdnspp_discovery::~mdnspp_discovery() = default;

void mdnspp_discovery::advertise(const plexus::discovery::service_info &service)
{
    ::mdnspp::service_info info;
    info.service_name = service.name;
    info.service_type = m_service_type;

    // The plexus endpoint address is "host:port"; split it back into the SRV
    // hostname/port + the A-record IPv4 the responder announces.
    const auto &addr = service.endpoint.address;
    auto colon = addr.rfind(':');
    if(colon != std::string::npos)
    {
        info.address_ipv4 = addr.substr(0, colon);
        info.port = static_cast<std::uint16_t>(std::stoul(addr.substr(colon + 1)));
    }
    info.hostname = service.name;
    info.txt_records = to_txt_records(service.metadata);

    m_impl->server = std::make_unique<::mdnspp::basic_service_server<::mdnspp::asio_policy>>(
        m_io, std::move(info));
    m_impl->server->async_start();
}

void mdnspp_discovery::browse(const resolved_callback &on_resolved)
{
    m_impl->browser = std::make_unique<::mdnspp::basic_service_discovery<::mdnspp::asio_policy>>(m_io);
    m_impl->browser->async_browse(
        m_service_type,
        [on_resolved](std::error_code, std::vector<::mdnspp::resolved_service> services)
        {
            for(const auto &svc : services)
            {
                plexus::discovery::service_info out;
                out.name = std::string{std::string_view{svc.instance_name}};
                out.endpoint = {"tcp", tcp_address(svc)};
                out.metadata = from_txt_entries(svc.txt_entries);
                on_resolved(out);
            }
        });
}

void mdnspp_discovery::stop()
{
    if(m_impl->server)
        m_impl->server->stop();
    if(m_impl->browser)
        m_impl->browser->stop();
}

}
