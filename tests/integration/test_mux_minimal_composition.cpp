#include "plexus/asio/asio_transport.h"
#include "plexus/asio/unix_transport.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/transport_backend.h"
#include "plexus/io/transport_selector.h"
#include "plexus/io/multiplexing_transport.h"

#include "plexus/policy.h"
#include "plexus/muxify.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

namespace pasio = plexus::asio;
namespace pio   = plexus::io;

using mux_pol = plexus::muxify<pasio::asio_policy>;

// The composability payoff: a node that wants ONLY the local (AF_UNIX) and remote (plain
// TCP) streams instantiates the core multiplexer over exactly that member subset — no
// secure (TLS) member, so this translation unit pulls in NEITHER plexus::crypto NOR
// OpenSSL. The all_backends_mux alias is the convenience for the full set; it is NOT the
// only composition. This proves the generic core composes an arbitrary member pack.
using minimal_mux = pio::multiplexing_transport<pasio::unix_transport, pasio::asio_transport>;

// The generic muxify<P> lift of the asio leaf Policy onto the erased mux channel must
// itself remain a Policy, and the minimal composition must satisfy the single erased
// transport_backend surface the engine drives over it — composing fewer members does not
// weaken the contract. These two static_asserts preserve the gate the deleted asio mux
// policy header carried, now at a living site.
static_assert(plexus::Policy<mux_pol>, "muxify<asio_policy> must satisfy Policy");
static_assert(pio::transport_backend<minimal_mux, mux_pol>,
              "the minimal unix+tcp multiplexer must satisfy transport_backend");

TEST_CASE("mux_minimal: a unix+tcp multiplexer composes, constructs, and routes by scheme without "
          "the secure member",
          "[integration][mux][minimal]")
{
    ::asio::io_context io;
    // The members are borrowed by reference and MUST outlive the mux (non-movable).
    pasio::unix_transport local{io};
    pasio::asio_transport remote{io};

    minimal_mux mux{local, remote};

    // Construction wired both members' completion callbacks. A listen/dial routes by
    // scheme over just these two members — exercising the route_of path on the minimal
    // pack (no secure member to fall through to). Neither call blocks: nothing is pumped,
    // so no connection is established; this proves the composition is live, not a network
    // round-trip (the live legs live in the all-backends oracles).
    mux.listen({"unix", "/tmp/pxm-minimal-does-not-exist/s"});
    mux.dial({"tcp", "127.0.0.1:0"});

    SUCCEED("the unix+tcp multiplexer composed and constructed without the secure backend");
}
