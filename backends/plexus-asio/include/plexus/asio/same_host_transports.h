#ifndef HPP_GUARD_PLEXUS_ASIO_SAME_HOST_TRANSPORTS_H
#define HPP_GUARD_PLEXUS_ASIO_SAME_HOST_TRANSPORTS_H

#include "plexus/asio/transport_set.h"
#include "plexus/asio/asio_transport.h"
#include "plexus/asio/asio_policy.h"
#include "plexus/asio/unix_transport.h"

#include "plexus/node.h"
#include "plexus/node_id.h"
#include "plexus/node_options.h"

#include "plexus/discovery/discovery.h"

// The single platform-selection point: shared memory is engaged where the host offers the
// accelerated same-host substrate. PLEXUS_SAME_HOST_NO_SHM forces the portable AF_UNIX + TCP
// branch on any host (it lets the non-shm composition be exercised off a Linux build host).
#if defined(__linux__) && !defined(PLEXUS_SAME_HOST_NO_SHM)
    #define PLEXUS_SAME_HOST_SHM 1
#else
    #define PLEXUS_SAME_HOST_SHM 0
#endif

#if PLEXUS_SAME_HOST_SHM
    #include "plexus/asio/shm/linux/shm_member.h"

    #include "plexus/native/machine_fingerprint.h"
    #include "plexus/native/posix_shm_region_broker.h"
#endif

#include <asio/io_context.hpp>

#include <utility>
#include <string>
#include <string_view>

namespace plexus::asio {

// A portable same-host composition: the consumer writes the SAME code on every platform and
// the most accelerated same-host substrate the platform offers is selected behind this header,
// with NO platform conditional in consumer code. On Linux the set is shm + AF_UNIX + TCP (the
// shared-memory fast path with the wire fallback); on every other platform it is AF_UNIX + TCP.
// The node type differs per platform, but the consumer holds it via `auto` and drives the
// identical node public API:
//
//   asio::io_context io;
//   plexus::discovery::static_discovery disc{...};
//   plexus::asio::same_host_transports ts{io};
//   auto node = ts.make_node(disc, id, plexus::node_options{});
//
// The Policy is fixed to plexus::asio::asio_policy internally — the production reactor for the
// composed leaves. The ctor takes a `region` on every platform for a uniform surface; only the
// Linux branch carries shared memory, where `region` is the shm-region NAMESPACE. Same-host shm
// region identity is derived from the topic name (the demand-driven region naming both peers
// converge on with no exchange), so by DEFAULT (an empty region) two peers — same process or two
// processes — share an shm ring by topic; the broker holds no cross-peer state.
//
// Set a DISTINCT region per application to ISOLATE its same-host shm from unrelated co-host apps:
// the namespace is folded deterministically into the region name, so two apps that pick different
// regions compute different region names for the same topic and never collide on a shared ring.
// It is static local config both peers of one application set identically, so isolation costs no
// wire exchange (convergence stays demand-driven). An empty region yields byte-identical region
// names to the namespace-less naming, so it shares by topic out of the box.
//
// LIFETIME: the set OWNS the leaves (and, on Linux, the shm region broker) and BORROWS the
// io_context. make_node returns a node that BORROWS those leaves, so this object MUST OUTLIVE
// every node minted from it (and the io_context must outlive this object) — the same
// single-owner teardown discipline transport_set and node.h document. Non-copyable /
// non-movable: the borrowed node leaves pin this object's address.
class same_host_transports
{
public:
#if PLEXUS_SAME_HOST_SHM
    using set_type = transport_set<shm::shm_member, unix_transport, asio_transport>;

    explicit same_host_transports(::asio::io_context &io, std::string_view region = "")
            : m_broker()
            , m_set(io, m_broker, std::string{region})
    {
    }
#else
    using set_type = transport_set<unix_transport, asio_transport>;

    explicit same_host_transports(::asio::io_context               &io,
                                  [[maybe_unused]] std::string_view region = "")
            : m_set(io)
    {
    }
#endif

    same_host_transports(const same_host_transports &)            = delete;
    same_host_transports &operator=(const same_host_transports &) = delete;
    same_host_transports(same_host_transports &&)                 = delete;
    same_host_transports &operator=(same_host_transports &&)      = delete;

#if PLEXUS_SAME_HOST_SHM
    // opts is taken BY VALUE: the default-fill is local to this call and never mutates the
    // caller's aggregate. A null (zero) local_fingerprint is the fillable "no same-host signal"
    // default (same_host.h null-guard); a caller-supplied value is respected, never clobbered,
    // so the explicit-override path stays intact.
    [[nodiscard]] auto make_node(discovery::discovery &disc, const plexus::node_id &id,
                                 node_options opts)
    {
        if(opts.handshake.local_fingerprint.is_null())
            opts.handshake.local_fingerprint = ::plexus::native::read_machine_fingerprint();
        return m_set.template make_node<asio_policy>(disc, id, opts);
    }
#else
    [[nodiscard]] auto make_node(discovery::discovery &disc, const plexus::node_id &id,
                                 const node_options &opts)
    {
        return m_set.template make_node<asio_policy>(disc, id, opts);
    }
#endif

private:
#if PLEXUS_SAME_HOST_SHM
    ::plexus::native::posix_shm_region_broker m_broker;
#endif
    set_type m_set;
};

}

#endif
