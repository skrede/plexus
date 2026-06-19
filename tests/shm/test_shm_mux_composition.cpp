#include "plexus/asio/shm/linux/all_backends_mux_shm.h"

#include "plexus/asio/all_backends_mux.h"

#include "plexus/io/transport_backend.h"

#include <catch2/catch_test_macros.hpp>

// The composition compile-gate: the shm-bearing multiplexer (shm joining as the SECOND
// local member alongside AF_UNIX) satisfies transport_backend, AND the existing no-shm
// all_backends_mux is INTACT (still a valid transport_backend) — the shm alias is purely
// additive. The static_asserts are the structural proof; the run-body is a token presence
// check so the case appears in the discovered list.

static_assert(plexus::io::transport_backend<plexus::asio::shm::all_backends_mux_shm,
                                            plexus::muxify<plexus::asio::asio_policy>>,
              "the shm-bearing composition must satisfy transport_backend");

static_assert(plexus::io::transport_backend<plexus::asio::all_backends_mux,
                                            plexus::muxify<plexus::asio::asio_policy>>,
              "the no-shm composition must stay intact (the shm alias is additive)");

TEST_CASE(
        "shm.mux_composition the shm-bearing and no-shm multiplexers both satisfy the backend seam",
        "[shm][mux][composition]")
{
    SUCCEED("the transport_backend static_asserts proved both compositions at compile time");
}
