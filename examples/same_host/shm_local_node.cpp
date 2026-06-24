// A live node that brings up shared memory as a same-host transport leaf. This is the
// worked recipe for composing shm + AF_UNIX + plain TCP without dragging in TLS/DTLS/UDP.
//
// THE TRANSPORT_SET PATH (the style this example demonstrates). A transport_set<Transports...>
// owns its leaves and mints a node that borrows them: the consumer names the transports once
// and constructs them with {io, broker} (or {io} when the pack carries no shm leaf), instead
// of hand-building each leaf and threading them through the node ctor. The set OWNS the leaves
// and MUST OUTLIVE every node it mints:
//
//   asio::io_context io;
//   plexus::native::posix_shm_region_broker broker;                  // owns the rings; outlives the
//   set plexus::asio::transport_set<plexus::asio::shm::shm_member,
//                               plexus::asio::unix_transport,
//                               plexus::asio::asio_transport> ts{io, broker};
//   auto node = ts.make_node<plexus::asio::asio_policy>(disc, id, plexus::node_options{});
//
// The set builds the shm leaf from {io, broker} (make_shm_member, found by ADL on the broker)
// and every plain leaf from {io}; the node it mints auto-installs the same-host preference hook
// (prefer shm, fall back to AF_UNIX on a failed ring acquire) because the pack carries an shm
// member. A no-shm pack uses the {io} ctor: transport_set<unix_transport, asio_transport>{io}.
//
// THE HAND-BUILT-LEAF ALTERNATIVE. The leaves can still be built and passed individually — the
// node with >=2 leaves builds its own multiplexer from the borrowed leaves:
//
//   auto shm = plexus::asio::shm::make_shm_member(io, broker);
//   plexus::asio::unix_transport local{io};
//   plexus::asio::asio_transport remote{io};
//   plexus::node<plexus::asio::asio_policy, plexus::asio::shm::shm_member,
//                plexus::asio::unix_transport, plexus::asio::asio_transport>
//       node{io, disc, id, shm, local, remote, plexus::node_options{}};
//
// THE PRE-BUILT-MUX ALTERNATIVE (plexus/asio/shm/linux/local_shm_mux.h). local_shm_mux is the
// lean, crypto-free alias for this three-member shape, handed to the node as a single leaf:
//
//   auto mux = plexus::asio::shm::make_local_shm_mux(shm, local, remote);
//   plexus::node<plexus::asio::asio_policy, local_shm_mux> node{io, disc, id, mux, opts};
//
// transport_set, the leaf pack, and local_shm_mux all include no tls/dtls/udp/openssl, so they
// build with PLEXUS_ENABLE_CRYPTO_BACKEND=OFF — the difference from all_backends_mux_shm, which
// carries the secure + datagram members.
//
// SCOPE: this example proves the composition COMPILES and a live shm-bearing node STANDS UP
// (constructs + listens) to rc=0. The same-host SHM delivery data path (two processes
// converging on a named ring and round-tripping a value byte-exact) is proven by the shm
// test suite — see tests/shm/test_shm_same_host_roundtrip.cpp and the live-mux xproc proofs
// in tests/shm/test_shm_large_message.cpp / test_shm_dual_delivery.cpp.

#include "plexus/asio/shm/linux/shm_member.h"
#include "plexus/asio/transport_set.h"
#include "plexus/asio/asio_transport.h"
#include "plexus/asio/unix_transport.h"

#include "plexus/node.h"
#include "plexus/node_options.h"

#include "plexus/native/posix_shm_region_broker.h"

#include "plexus/discovery/static_discovery.h"

#include <asio/io_context.hpp>

#include <string>
#include <cstddef>
#include <iostream>

namespace pasio = plexus::asio;

int main()
{
    ::asio::io_context io;
    plexus::native::posix_shm_region_broker broker;
    plexus::discovery::static_discovery disc{{}};

    pasio::transport_set<pasio::shm::shm_member, pasio::unix_transport, pasio::asio_transport> ts{
        io, broker};

    plexus::node_id id{};
    id[0]     = std::byte{0x2A};
    auto node = ts.make_node<pasio::asio_policy>(disc, id, plexus::node_options{});
    std::cout << "shm-bearing node minted from a transport_set (shm + unix + tcp leaves)\n";

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
