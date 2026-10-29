#include "test_unclaimed_retire_common.h"
#if defined(PLEXUS_HAVE_CRYPTO)
    #include "tls_retire_drain_common.h"
#endif
#if defined(PLEXUS_HAVE_SHM)
    #include "support/posted_queue.h"
    #include "support/noop_notifier.h"
#endif

#include "plexus/asio/asio_policy.h"
#include "plexus/asio/udp_channel.h"
#include "plexus/asio/udp_transport.h"
#include "plexus/asio/asio_transport.h"

#if defined(PLEXUS_HAVE_CRYPTO)
    #include "plexus/tls/dtls_channel.h"
    #include "plexus/tls/dtls_transport.h"
#endif

#if defined(PLEXUS_HAVE_SHM)
    #include "plexus/shm/shm_mux_member.h"

    #include "plexus/native/shm_region_broker.h"
#endif

#include "plexus/inproc/inproc_channel.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#if defined(PLEXUS_HAVE_SHM)
    #include "plexus/io/congestion.h"
    #include "plexus/io/reliability.h"
#endif
#include "plexus/io/multiplexing_transport.h"
#include "plexus/io/wire_capturing_transport.h"

#include "plexus/testing/platform.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <asio/ip/tcp.hpp>

#include <memory>
#include <string>
#include <utility>

namespace pasio   = plexus::asio;
namespace pio     = plexus::io;
namespace pinproc = plexus::inproc;

using namespace unclaimed_retire;
using tcp = ::asio::ip::tcp;

TEST_CASE("unclaimed_retire a udp transport with no consumer keeps an accepted channel until its on_closed", "[integration][unclaimed_retire][udp]")
{
    ::asio::io_context io;
    pasio::udp_transport server{io};
    pasio::udp_transport client{io};
    holder<pasio::udp_channel> held;

    run_accept_unclaimed(io, server, client, held, "udp");
    CHECK(held.channel);
}

TEST_CASE("unclaimed_retire a udp transport with no consumer keeps a dialed channel until its on_closed", "[integration][unclaimed_retire][udp]")
{
    ::asio::io_context io;
    pasio::udp_transport server{io};
    pasio::udp_transport client{io};
    holder<pasio::udp_channel> held;

    run_dial_unclaimed(io, client, server, held, "udp");
    CHECK(held.channel);
}

#if defined(PLEXUS_HAVE_CRYPTO)
namespace ptls = plexus::tls;

TEST_CASE("unclaimed_retire a dtls transport with no consumer keeps an accepted channel until its on_closed", "[integration][unclaimed_retire][dtls]")
{
    tls_retire_drain::mutual_dtls_credentials creds;
    ::asio::io_context io;
    ptls::dtls_transport server{io, creds.server};
    ptls::dtls_transport client{io, creds.client};
    holder<ptls::dtls_channel> held;

    run_accept_unclaimed(io, server, client, held, "dtls");
    CHECK(held.channel);
}

TEST_CASE("unclaimed_retire a dtls transport with no consumer keeps a dialed channel until its on_closed", "[integration][unclaimed_retire][dtls]")
{
    tls_retire_drain::mutual_dtls_credentials creds;
    ::asio::io_context io;
    ptls::dtls_transport server{io, creds.server};
    ptls::dtls_transport client{io, creds.client};
    holder<ptls::dtls_channel> held;

    run_dial_unclaimed(io, client, server, held, "dtls");
    CHECK(held.channel);
}
#endif

TEST_CASE("unclaimed_retire an inproc transport with no consumer keeps an accepted end until its on_closed", "[integration][unclaimed_retire][inproc]")
{
    inproc_link link;
    link.dialer.on_dialed([&link](std::unique_ptr<pinproc::inproc_channel<>> ch, const pio::endpoint &) { link.take(std::move(ch)); });
    link.dialer.dial({"inproc", "unclaimed_retire"});
    link.ex.drain();

    REQUIRE(link.held);
    CHECK(link.broken);
}

TEST_CASE("unclaimed_retire an inproc transport with no consumer keeps a dialed end until its on_closed", "[integration][unclaimed_retire][inproc]")
{
    inproc_link link;
    link.listener.on_accepted([&link](std::unique_ptr<pinproc::inproc_channel<>> ch) { link.take(std::move(ch)); });
    link.dialer.dial({"inproc", "unclaimed_retire"});
    link.ex.drain();

    REQUIRE(link.held);
    CHECK(link.broken);
}

#if defined(PLEXUS_HAVE_SHM)
using shm_member = plexus::shm::shm_mux_member<plexus::native::shm_region_broker, plexus::testing::noop_notifier>;

TEST_CASE("unclaimed_retire a shm member with no consumer keeps its channels until their on_closed", "[integration][unclaimed_retire][shm]")
{
    const std::string tag = "unclaimed_retire." + std::to_string(plexus::testing::process_id());
    plexus::testing::posted_queue queue;
    plexus::native::shm_region_broker broker;
    shm_member shm(broker, queue.post(), pio::reliability::reliable, pio::congestion::block);
    shm.listen({"shm", tag + ".listen"});
    shm.dial({"shm", tag + ".dial"});
    queue.drain();

    CHECK(shm.registry().live_count() == 0);
}
#endif

TEST_CASE("unclaimed_retire a multiplexing transport with no consumer keeps an accepted channel until its on_closed", "[integration][unclaimed_retire][mux]")
{
    ::asio::io_context io;
    pasio::asio_transport member{io};
    pio::multiplexing_transport<pasio::asio_transport> mux{member};
    mux.listen({"tcp", "127.0.0.1:0"});

    CHECK(peer_sees_close<tcp::socket>(io, tcp::endpoint{::asio::ip::make_address("127.0.0.1"), member.port()}));
}

TEST_CASE("unclaimed_retire a multiplexing transport with no consumer keeps a dialed channel until its on_closed", "[integration][unclaimed_retire][mux]")
{
    ::asio::io_context io;
    pasio::asio_transport member{io};
    pio::multiplexing_transport<pasio::asio_transport> mux{member};

    CHECK(tcp_dial_sees_close(io, mux));
}

TEST_CASE("unclaimed_retire a wire-capturing transport with no consumer keeps an accepted channel until its on_closed", "[integration][unclaimed_retire][wire_capture]")
{
    ::asio::io_context io;
    pasio::asio_transport inner{io};
    pio::wire_capturing_transport<pasio::asio_transport, pasio::asio_policy> capture{inner};
    capture.on_accepted({});
    capture.listen({"tcp", "127.0.0.1:0"});

    CHECK(peer_sees_close<tcp::socket>(io, tcp::endpoint{::asio::ip::make_address("127.0.0.1"), inner.port()}));
}

TEST_CASE("unclaimed_retire a wire-capturing transport with no consumer keeps a dialed channel until its on_closed", "[integration][unclaimed_retire][wire_capture]")
{
    ::asio::io_context io;
    pasio::asio_transport inner{io};
    pio::wire_capturing_transport<pasio::asio_transport, pasio::asio_policy> capture{inner};
    capture.on_dialed({});

    CHECK(tcp_dial_sees_close(io, capture));
}
