#ifndef HPP_GUARD_PLEXUS_MDNSPP_MDNSPP_DISCOVERY_H
#define HPP_GUARD_PLEXUS_MDNSPP_MDNSPP_DISCOVERY_H

#include "plexus/discovery/discovery.h"

#include <asio/io_context.hpp>

#include <string>
#include <memory>

namespace plexus::mdnspp {

// mDNS-backed discovery adapter: the production form of the cold-path discovery
// seam. It implements plexus::discovery::discovery over mdnspp's asio-policy
// service server (advertise) and service discovery (browse), translating
// mdnspp's resolved services into plexus service_info on the resolved-callback.
//
// Constructed from an EXTERNAL asio::io_context the caller owns — it never
// creates its own context. This is the shared-executor entry point: the same
// io_context that drives the plexus asio transport drives the mDNS sockets, so
// a discovery resolve and a transport round-trip both progress on one context
// (the locked "mdnspp shares the executor" requirement).
//
// The mdnspp types are heavy and C++23, so the members are held behind an
// opaque impl and the mdnspp headers stay out of this interface; only the
// asio::io_context reference and the plexus discovery vocabulary cross the seam.
class mdnspp_discovery final : public discovery::discovery
{
public:
    // service_type is the DNS-SD service type the adapter advertises/browses,
    // e.g. "_plexus._tcp.local.". The io_context is borrowed, not owned.
    mdnspp_discovery(::asio::io_context &io, std::string service_type);
    ~mdnspp_discovery() override;

    mdnspp_discovery(const mdnspp_discovery &)            = delete;
    mdnspp_discovery &operator=(const mdnspp_discovery &) = delete;

    // Publish a local service so peers can resolve it (mDNS announce).
    void advertise(const plexus::discovery::service_info &service) override;

    // Start an mDNS browse; on_resolved fires once per resolved peer, carrying
    // the peer's name and the tcp endpoint reconstructed from its mDNS records.
    void browse(const resolved_callback &on_resolved) override;

    // Stop advertising and browsing (idempotent).
    void stop() override;

private:
    struct impl;

    ::asio::io_context   &m_io;
    std::string           m_service_type;
    std::unique_ptr<impl> m_impl;
};

}

#endif
