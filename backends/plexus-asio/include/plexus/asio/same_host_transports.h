#ifndef HPP_GUARD_PLEXUS_ASIO_SAME_HOST_TRANSPORTS_H
#define HPP_GUARD_PLEXUS_ASIO_SAME_HOST_TRANSPORTS_H

#include "plexus/asio/transport_set.h"
#include "plexus/asio/asio_transport.h"
#include "plexus/asio/asio_policy.h"
#include "plexus/asio/unix_transport.h"
#include "plexus/asio/detail/same_host_shm_config.h"

#include "plexus/node.h"
#include "plexus/node_id.h"
#include "plexus/node_options.h"

#include "plexus/discovery/discovery.h"

#if PLEXUS_SAME_HOST_SHM
    #include "plexus/native/machine_fingerprint.h"
    #include "plexus/native/posix_shm_region_broker.h"
#endif

#include <asio/io_context.hpp>

#include <utility>
#include <string>
#include <string_view>

namespace plexus::asio {

// A portable same-host composition: the most accelerated same-host substrate the platform
// offers is selected behind this header, with no platform conditional in consumer code — shm +
// AF_UNIX + TCP on Linux, AF_UNIX + TCP elsewhere. The ctor's `region` is the shm-region
// namespace (Linux only): an empty region shares rings by topic, a distinct region isolates this
// composition's same-host shm from unrelated co-host apps.
//
// LIFETIME: the set OWNS the leaves (and, on Linux, the shm region broker) and BORROWS the
// io_context. make_node returns a node that BORROWS those leaves, so this object MUST OUTLIVE
// every node minted from it (and the io_context must outlive this object). Non-copyable /
// non-movable: the borrowed node leaves pin this object's address.
class same_host_transports
{
public:
#if PLEXUS_SAME_HOST_SHM
    using set_type       = transport_set<shm::shm_member, unix_transport, asio_transport>;
    using region_broker_t = typename shm::shm_member::registry_type::broker_type;

    explicit same_host_transports(::asio::io_context &io, std::string_view region = "")
            : m_broker()
            , m_set(io, m_broker, std::string{region})
    {
    }
#else
    using set_type = transport_set<unix_transport, asio_transport>;

    explicit same_host_transports(::asio::io_context &io, [[maybe_unused]] std::string_view region = "")
            : m_set(io)
    {
    }
#endif

    same_host_transports(const same_host_transports &)            = delete;
    same_host_transports &operator=(const same_host_transports &) = delete;
    same_host_transports(same_host_transports &&)                 = delete;
    same_host_transports &operator=(same_host_transports &&)      = delete;

#if PLEXUS_SAME_HOST_SHM
    // opts is taken by value: the default-fill of an absent local_fingerprint is local to this
    // call and never mutates the caller's aggregate; a caller-supplied value is left untouched.
    node<asio_policy, shm::shm_member, unix_transport, asio_transport> make_node(discovery::discovery &disc, const plexus::node_id &id, node_options opts)
    {
        if(opts.handshake.local_fingerprint.is_null())
            opts.handshake.local_fingerprint = ::plexus::native::read_machine_fingerprint();
        return m_set.template make_node<asio_policy>(disc, id, opts);
    }
#else
    node<asio_policy, unix_transport, asio_transport> make_node(discovery::discovery &disc, const plexus::node_id &id, const node_options &opts)
    {
        return m_set.template make_node<asio_policy>(disc, id, opts);
    }
#endif

private:
#if PLEXUS_SAME_HOST_SHM
    region_broker_t m_broker;
#endif
    set_type m_set;
};

}

#endif
