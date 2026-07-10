#ifndef HPP_GUARD_PLEXUS_ASIO_DEFAULT_DISCOVERY_H
#define HPP_GUARD_PLEXUS_ASIO_DEFAULT_DISCOVERY_H

#include "plexus/asio/asio_policy.h"
#include "plexus/asio/udp_multicast_socket.h"
#include "plexus/asio/detail/interface_resolve.h"

#include "plexus/discovery/discovery.h"
#include "plexus/discovery/discovery_health.h"
#include "plexus/discovery/discovery_options.h"
#include "plexus/discovery/multicast_discovery.h"
#include "plexus/log/logger.h"
#include "plexus/io/congestion.h"

#include <asio/io_context.hpp>
#include <asio/ip/address_v4.hpp>

#include <chrono>
#include <string>
#include <cstddef>
#include <utility>
#include <system_error>

namespace plexus::asio {

// The one-line out-of-box discovery composition: it BORROWS the io_context and OWNS the multicast
// socket + the first-party discovery leaf over it, resolving the egress selector once at
// construction. discovery() hands the leaf out by reference for a node to browse/advertise, and
// self_check() folds the resolve result and the leaf's self-echo probe into a single
// discovery_health so the two silent-failure modes (loopback off, unresolved interface) surface as
// a queryable state plus one logged warning.
//
// LIFETIME: mirrors same_host_transports — non-copyable / non-movable, the borrowed leaf pins this
// object's address, so it MUST OUTLIVE every node that browses its discovery() (and the io_context
// must outlive it).
class default_discovery
{
public:
    using leaf_type    = discovery::multicast_discovery<udp_multicast_socket, asio_policy>;
    using options_type = discovery::discovery_options;
    using health_type  = discovery::discovery_health;

    explicit default_discovery(::asio::io_context &io, options_type options = {})
            : default_discovery(io, std::move(options), nullptr)
    {
    }

    default_discovery(::asio::io_context &io, options_type options, log::logger &log)
            : default_discovery(io, std::move(options), &log)
    {
    }

    default_discovery(const default_discovery &)            = delete;
    default_discovery &operator=(const default_discovery &) = delete;
    default_discovery(default_discovery &&)                 = delete;
    default_discovery &operator=(default_discovery &&)      = delete;

    ::plexus::discovery::discovery &discovery()
    {
        return m_leaf;
    }

    // Non-blocking: meaningful only once io.run() has pumped long enough for the node's own echo to
    // return; a non-healthy TERMINAL verdict logs exactly one warning naming the likely cause.
    health_type self_check() const
    {
        const health_type health = compute_health();
        warn_once(health);
        return health;
    }

private:
    default_discovery(::asio::io_context &io, options_type options, log::logger *log)
            : m_options(std::move(options))
            , m_default_logger()
            , m_socket(io, parse_group(m_options.group), m_options.port, m_options.ttl, io::congestion::block, k_send_queue_bytes, m_options.egress_interface)
            , m_leaf(io, m_socket, m_options)
            , m_log(log != nullptr ? *log : m_default_logger)
            , m_resolve_ec(detail::resolve_interface(m_options.egress_interface).ec)
            , m_window(m_options.announce_period * k_probe_window_periods)
            , m_warned(false)
    {
    }

    static ::asio::ip::address_v4 parse_group(const std::string &text)
    {
        std::error_code ec;
        const ::asio::ip::address_v4 addr = ::asio::ip::make_address_v4(text, ec);
        return ec ? ::asio::ip::address_v4{} : addr;
    }

    health_type compute_health() const
    {
        if(m_resolve_ec)
            return health_type::bad_interface;
        return m_leaf.probe(std::chrono::steady_clock::now(), m_window);
    }

    void warn_once(health_type health) const
    {
        if(m_warned || health == health_type::healthy || health == health_type::not_yet)
            return;
        m_log.warn(health == health_type::bad_interface ? "plexus: discovery egress interface did not resolve (check the interface selector)"
                                                        : "plexus: discovery saw no self-echo (multicast loopback disabled?)");
        m_warned = true;
    }

    static constexpr std::size_t k_send_queue_bytes = 65536;
    // A conservative self-echo window: several announce periods, far above the sub-millisecond
    // loopback echo latency, so a node that is genuinely pumping its loop is never falsely read
    // no_self_echo on a slow or loaded host.
    static constexpr int k_probe_window_periods = 5;

    options_type m_options;
    log::null_logger m_default_logger;
    udp_multicast_socket m_socket;
    leaf_type m_leaf;
    log::logger &m_log;
    std::error_code m_resolve_ec;
    std::chrono::milliseconds m_window;
    mutable bool m_warned;
};

}

#endif
