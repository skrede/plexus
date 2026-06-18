// A live node that brings up shared memory as a same-host transport leaf. This is the
// worked recipe for composing shm + AF_UNIX + plain TCP without dragging in TLS/DTLS/UDP.
//
// COMPOSING SHM AS A VARIADIC LEAF (the style this example uses, and the one that compiles
// cleanly on the GCC baseline). A node<Policy, Transports...> with >=2 leaves builds its
// own multiplexer internally from the borrowed leaves; the consumer writes the plain
// asio_policy and lists the leaves, and the node computes the muxify<asio_policy> engine
// policy itself. To add shared memory:
//
//   asio::io_context io;
//   plexus::shm::posix_shm_region_broker broker;                 // owns the rings; outlives the node
//   auto shm = plexus::asio::shm::make_shm_member(io, broker);   // shm leaf (broker + reactor binder)
//   plexus::asio::unix_transport local{io};                      // AF_UNIX same-host fallback
//   plexus::asio::asio_transport remote{io};                     // plain-TCP cross-host stream
//   using shm_node = plexus::node<plexus::asio::asio_policy, plexus::asio::shm::shm_member,
//                                 plexus::asio::unix_transport, plexus::asio::asio_transport>;
//   shm_node node{io, disc, id, shm, local, remote, plexus::node_options{}};
//
// make_shm_member is the recipe: it binds the shm_mux_member over the POSIX region broker
// and a notifier-binder that captures `io`, so each ring's io_uring-futex bridge wakes on
// that reactor. The broker is BORROWED — it must outlive the member, which must outlive the
// node. The node auto-installs the same-host preference hook (prefer shm, fall back to
// AF_UNIX on a failed ring acquire) for any composition that carries an shm member, so the
// consumer wires nothing extra.
//
// THE PRE-BUILT-MUX ALTERNATIVE (plexus/asio/shm/linux/local_shm_mux.h). If you would
// rather hand the node a single ready multiplexer than a leaf pack, local_shm_mux is the
// lean, crypto-free alias for exactly this three-member shape:
//
//   using local_shm_mux = io::multiplexing_transport<shm_member, unix_transport, asio_transport>;
//   auto mux = plexus::asio::shm::make_local_shm_mux(shm, local, remote);
//   plexus::node<plexus::asio::asio_policy, local_shm_mux> node{io, disc, id, mux, opts};
//
// It includes no tls/dtls/udp/openssl, so it builds with PLEXUS_ENABLE_CRYPTO_BACKEND=OFF —
// the difference from all_backends_mux_shm, which carries the secure + datagram members.
//
// SCOPE: this example proves the composition COMPILES and a live shm-bearing node STANDS UP
// (constructs + listens) to rc=0. The same-host SHM delivery data path (two processes
// converging on a named ring and round-tripping a value byte-exact) is proven by the shm
// test suite — see tests/shm/test_shm_same_host_roundtrip.cpp and the live-mux xproc proofs
// in tests/shm/test_shm_large_message.cpp / test_shm_dual_delivery.cpp.

#include "plexus/asio/shm/linux/shm_member.h"
#include "plexus/asio/asio_transport.h"
#include "plexus/asio/unix_transport.h"

#include "plexus/node.h"
#include "plexus/node_options.h"

#include "plexus/shm/posix_shm_region_broker.h"

#include "plexus/discovery/static_discovery.h"

#include <asio/io_context.hpp>

#include <string>
#include <cstddef>
#include <iostream>

namespace pasio = plexus::asio;

int main()
{
    using shm_node = plexus::node<pasio::asio_policy, pasio::shm::shm_member,
                                  pasio::unix_transport, pasio::asio_transport>;

    ::asio::io_context io;
    plexus::shm::posix_shm_region_broker broker;
    plexus::discovery::static_discovery disc{{}};

    auto shm = pasio::shm::make_shm_member(io, broker);
    pasio::unix_transport local{io};
    pasio::asio_transport remote{io};

    plexus::node_id id{};
    id[0] = std::byte{0x2A};
    shm_node node{io, disc, id, shm, local, remote, plexus::node_options{}};
    std::cout << "shm-bearing node constructed (shm + unix + tcp leaves)\n";

    // Bring the same-host AF_UNIX listener up: the node stands up a real local endpoint,
    // proving the composed substrate listens. The shm leaf mints its rings on demand (a
    // dial), so a single-node bring-up exercises construction + listen, not a ring acquire.
    const std::string sock = "/tmp/plexus-shm-local-node-" + std::to_string(::getpid()) + ".sock";
    node.listen({"unix", sock});
    io.poll();
    std::cout << "node listening on unix:" << sock << "\n";

    io.poll();
    std::cout << "shm local node example complete\n";
    return 0;
}
