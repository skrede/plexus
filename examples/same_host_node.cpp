// The portable same-host node: ONE consumer surface that works on every platform with NO
// platform conditional in this file. plexus::asio::same_host_transports resolves, behind its
// header, to the most accelerated same-host substrate the host offers — shm + AF_UNIX + TCP on
// Linux, AF_UNIX + TCP elsewhere — and mints a node the consumer holds via `auto` and drives
// through the identical node public API. The accelerated leaf is engaged where it works; the
// consumer writes plexus and nothing platform-specific.
//
//   asio::io_context io;
//   plexus::discovery::static_discovery disc{...};
//   plexus::asio::same_host_transports ts{io};           // empty region: peers share by topic
//   // plexus::asio::same_host_transports ts{io, "my-app"};  // a region isolates same-host shm
//   auto node = ts.make_node(disc, id, plexus::node_options{});
//
// The optional `region` is the shm-region NAMESPACE: empty (the default) lets two peers share an
// shm ring by topic; a DISTINCT region per application isolates its same-host shm so two unrelated
// co-host apps using the same topic names never collide on a shared region.
//
// The Policy is fixed to plexus::asio::asio_policy inside same_host_transports. The set OWNS
// its leaves (and, on Linux, the shm region broker) and MUST OUTLIVE every node it mints; the
// io_context must outlive the set — the same single-owner teardown discipline node.h documents.
//
// SCOPE: this example proves the portable composition COMPILES and a live node STANDS UP
// (constructs + listens) to rc=0. The same-host shm delivery data path — two peers converging
// on a named ring and round-tripping a value byte-exact — is proven by the shm test suite (see
// tests/shm/test_same_host_transports.cpp and tests/shm/test_shm_same_host_roundtrip.cpp).

#include "plexus/asio/same_host_transports.h"

#include "plexus/node_options.h"

#include "plexus/discovery/static_discovery.h"

#include <asio/io_context.hpp>

#include <string>
#include <cstddef>
#include <iostream>

namespace pasio = plexus::asio;

int main()
{
    ::asio::io_context                  io;
    plexus::discovery::static_discovery disc{{}};

    pasio::same_host_transports ts{io};

    plexus::node_id id{};
    id[0]     = std::byte{0x2A};
    auto node = ts.make_node(disc, id, plexus::node_options{});
    std::cout << "portable same-host node minted (accelerated where the host offers it)\n";

    const std::string sock = "/tmp/plexus-same-host-node-" + std::to_string(::getpid()) + ".sock";
    node.listen({"unix", sock});
    io.poll();
    std::cout << "node listening on unix:" << sock << "\n";

    io.poll();
    std::cout << "portable same-host node example complete\n";
    return 0;
}
