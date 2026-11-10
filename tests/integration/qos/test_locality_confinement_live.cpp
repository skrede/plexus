#include "test_locality_confinement_live_common.h"

#include <catch2/catch_test_macros.hpp>

#ifdef PLEXUS_HAVE_ASIO_MUX

using namespace locality_confinement_fixture;

TEST_CASE("locality confinement (live AF_UNIX): a remote-confined topic never crosses the local "
          "stream; a local one does, looped",
          "[integration][locality][confinement][unix]")
{
    constexpr int k_iterations = 100;
    int proven                 = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        temp_sock sock;
        live_link<pasio::unix_policy, pasio::unix_transport, pasio::unix_channel> l;
        l.wire();
        l.transport.listen({"unix", sock.path});
        l.transport.dial({"unix", sock.path});

        // The AF_UNIX channel classifies as the LOCAL tier: a remote-only topic is dropped,
        // a process|local topic (which includes local) is delivered.
        prove_link_confinement(l, "live.unix.topic",
                               /*including=*/locality::process | locality::local,
                               /*excluding=*/locality::remote);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("locality confinement (live TCP): a local-confined topic never crosses the network "
          "stream; a remote one does, looped",
          "[integration][locality][confinement][tcp]")
{
    constexpr int k_iterations = 100;
    int proven                 = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        live_link<pasio::asio_policy, pasio::asio_transport, pasio::asio_channel> l;
        l.wire();
        l.transport.listen({"tcp", "127.0.0.1:0"});
        const std::uint16_t port = l.transport.port();
        l.transport.dial({"tcp", "127.0.0.1:" + std::to_string(port)});

        // The TCP channel classifies as the REMOTE tier: a process|local topic is dropped,
        // a remote topic is delivered.
        prove_link_confinement(l, "live.tcp.topic",
                               /*including=*/locality::remote,
                               /*excluding=*/locality::process | locality::local);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

#endif
