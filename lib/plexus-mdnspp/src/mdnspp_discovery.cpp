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
#include <charconv>
#include <string_view>
#include <system_error>

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

    ::asio::io_context                                                      &context;
    std::unique_ptr<::mdnspp::basic_service_server<::mdnspp::AsioPolicy>>    server;
    std::unique_ptr<::mdnspp::basic_service_discovery<::mdnspp::AsioPolicy>> browser;
    std::string advertised_name; // the live server's service name, for the in-place-update decision
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
std::vector<::mdnspp::service_txt>
to_txt_records(const std::vector<std::pair<std::string, std::string>> &metadata)
{
    std::vector<::mdnspp::service_txt> records;
    records.reserve(metadata.size());
    for(const auto &[key, value] : metadata)
        records.push_back(::mdnspp::service_txt{key, value});
    return records;
}

std::vector<std::pair<std::string, std::string>>
from_txt_entries(const std::vector<::mdnspp::service_txt> &entries)
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
    // hostname/port + the A-record IPv4 the responder announces. The port tail is parsed
    // with from_chars into a uint16 (mirroring contact_card::read_transport_port) so a
    // non-numeric, out-of-range, or trailing-garbage port skips the advertisement rather
    // than throwing (std::stoul) or silently truncating a >65535 value into a wrong port.
    const auto &addr  = service.endpoint.address;
    auto        colon = addr.rfind(':');
    if(colon != std::string::npos)
    {
        const auto    port_tail = std::string_view{addr}.substr(colon + 1);
        std::uint16_t port{};
        const char   *first  = port_tail.data();
        const char   *last   = port_tail.data() + port_tail.size();
        const auto    parsed = std::from_chars(first, last, port);
        if(parsed.ec != std::errc{} || parsed.ptr != last)
            return;
        info.address_ipv4 = addr.substr(0, colon);
        info.port         = port;
    }
    else if(!addr.empty())
    {
        // A host-only address (the node advertises its reachable host and carries the
        // real per-transport port in the contact-card TXT keys, not the SRV port). The
        // A record still MUST carry the host or a browser resolves the service by name
        // with no address and cannot dial it.
        info.address_ipv4 = addr;
    }
    info.hostname    = service.name;
    info.txt_records = to_txt_records(service.metadata);

    // A re-advertise on a LIVE server with an UNCHANGED service name updates the record
    // in place (an RFC 6762 section 8.4 announcement burst), instead of tearing the
    // server down and re-probing — the goodbye + re-probe flap a fresh server causes.
    // The hostname tracks the service name here, so an unchanged name leaves both
    // unchanged and mdnspp takes the no-reprobe path. A first advertise or a changed
    // name creates the server.
    if(m_impl->server && service.name == m_impl->advertised_name)
    {
        m_impl->server->update_service_info(std::move(info));
        return;
    }

    m_impl->advertised_name = service.name;
    m_impl->server = std::make_unique<::mdnspp::basic_service_server<::mdnspp::AsioPolicy>>(
            m_io, std::move(info));
    m_impl->server->async_start();
}

void mdnspp_discovery::browse(const resolved_callback &on_resolved)
{
    m_impl->browser =
            std::make_unique<::mdnspp::basic_service_discovery<::mdnspp::AsioPolicy>>(m_io);
    m_impl->browser->async_browse(
            m_service_type,
            [on_resolved](std::error_code, std::vector<::mdnspp::resolved_service> services)
            {
                for(const auto &svc : services)
                {
                    plexus::discovery::service_info out;
                    out.name     = std::string{std::string_view{svc.instance_name}};
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
