#include "plexus/asio/same_host_transports.h"

#include "plexus/shm/machine_fingerprint.h"

#include "plexus/io/host_fingerprint.h"

#include "plexus/node_options.h"

#include "plexus/discovery/static_discovery.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <cstddef>

// The null-fingerprint silent no-op fix: same_host_transports::make_node default-fills
// the node's local same-host fingerprint from the real machine read when the caller left
// it null, so both ends of a same-host handshake finally compare a real, equal value (the
// is_same_host null-guard no longer fails closed on a never-populated fingerprint). The
// composition respects a caller-supplied fingerprint verbatim — the explicit-override path
// the inproc oracle relies on stays intact. The effective fingerprint is read off the
// minted node's engine (the configured local_fingerprint the handshake advertises).

namespace pasio = plexus::asio;
namespace pio   = plexus::io::shm;

TEST_CASE("shm.same_host_fingerprint_node make_node default-fills the real machine fingerprint",
          "[shm][node][same_host][fingerprint]")
{
    ::asio::io_context                  io;
    plexus::discovery::static_discovery disc{{}};

    pasio::same_host_transports ts{io};

    plexus::node_id id{};
    id[0]     = std::byte{0x3C};
    auto node = ts.make_node(disc, id, plexus::node_options{});

    const plexus::io::host_fingerprint effective = node.router().local_fingerprint();
    REQUIRE_FALSE(effective.is_null());
    REQUIRE(effective == plexus::shm::read_machine_fingerprint());
}

TEST_CASE("shm.same_host_fingerprint_node make_node preserves an explicit fingerprint",
          "[shm][node][same_host][fingerprint]")
{
    ::asio::io_context                  io;
    plexus::discovery::static_discovery disc{{}};

    pasio::same_host_transports ts{io};

    plexus::node_id id{};
    id[0] = std::byte{0x4D};

    plexus::node_options        opts{};
    const plexus::io::host_fingerprint forced{plexus::shm::read_machine_fingerprint().value ^ 0xA5A5A5A5u};
    opts.handshake.local_fingerprint = forced;
    REQUIRE_FALSE(forced.is_null());

    auto node = ts.make_node(disc, id, opts);

    REQUIRE(node.router().local_fingerprint() == forced);
}
