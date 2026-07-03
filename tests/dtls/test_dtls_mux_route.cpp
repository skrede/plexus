#include "test_dtls_mux_common.h"

#include "plexus/testing/platform.h"

using namespace dtls_mux_fixture;

TEST_CASE("dtls.mux: a tcp dial on the same mux still routes to the plain-TCP member — no "
          "cross-talk, looped",
          "[dtls][mux][route]")
{
    pdt::identity_fixture server_id("xt_srv");
    pdt::identity_fixture client_id("xt_cli");

    int completed = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        // A mux that carries a dtls member also routes a plain "tcp" dial to the
        // plaintext stream member — the secure-datagram member never intercepts it.
        mux_pair n(server_id, client_id);
        n.listen_face.mux.listen({"tcp", "127.0.0.1:0"});
        n.dial_face.mux.dial({"tcp", "127.0.0.1:" + std::to_string(n.listen_face.remote.port())});
        n.pump_until([&] { return n.dialed && n.accepted; });

        REQUIRE(n.dialed != nullptr);
        REQUIRE(n.accepted != nullptr);
        REQUIRE(n.dialed->remote_endpoint().scheme == "tcp");
        REQUIRE(n.accepted->remote_endpoint().scheme == "tcp");
        REQUIRE(n.dialed_ep.has_value());
        REQUIRE(n.dialed_ep->scheme == "tcp");
        ++completed;
    }
    REQUIRE(completed == k_iterations);
}

TEST_CASE("dtls.mux: a tls dial on the same mux still routes to the secure-stream member — "
          "coexists with dtls, looped",
          "[dtls][mux][route]")
{
    pdt::identity_fixture server_id("xs_srv");
    pdt::identity_fixture client_id("xs_cli");

    int completed = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        // The mux's secure-stream (tls) member and secure-datagram (dtls) member coexist:
        // a "tls" dial reaches the tls member, not the dtls member.
        mux_pair n(server_id, client_id);
        n.listen_face.mux.listen({"tls", "127.0.0.1:0"});
        n.dial_face.mux.dial({"tls", "127.0.0.1:" + std::to_string(n.listen_face.secure.port())});
        n.pump_until([&] { return n.dialed && n.accepted; });

        REQUIRE(n.dialed != nullptr);
        REQUIRE(n.accepted != nullptr);
        REQUIRE(n.dialed->remote_endpoint().scheme == "tls");
        REQUIRE(n.accepted->remote_endpoint().scheme == "tls");
        ++completed;
    }
    REQUIRE(completed == k_iterations);
}

TEST_CASE("dtls.mux: a same-host (unix) dial routes to the local member, never the dtls member, "
          "looped",
          "[dtls][mux][route]")
{
    pdt::identity_fixture server_id("lx_srv");
    pdt::identity_fixture client_id("lx_cli");

    // The locality-exclusion behavioral proof: a same-host scheme classifies local and
    // routes to the AF_UNIX local member — never the remote secure-datagram (dtls) member,
    // because the dtls route branch sits AFTER the locality short-circuit in route_of. A
    // process|local-confined topic (which carries a same-host endpoint) thus never egresses
    // over dtls. Each iteration uses a fresh ephemeral socket path so the runs are isolated.
    int completed = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        const std::string path = std::filesystem::temp_directory_path() / ("plexus_dtls_mux_unix_" + std::to_string(plexus::testing::process_id()) + "_" + std::to_string(iter) + ".sock");
        std::error_code rc;
        std::filesystem::remove(path, rc);

        mux_pair n(server_id, client_id);
        n.listen_face.mux.listen({"unix", path});
        n.dial_face.mux.dial({"unix", path});
        n.pump_until([&] { return n.dialed && n.accepted; });

        REQUIRE(n.dialed != nullptr);
        REQUIRE(n.accepted != nullptr);
        // The same-host route landed on the unix member — the scheme survives as "unix",
        // proving the locality short-circuit won over any remote (dtls) classification.
        REQUIRE(n.dialed->remote_endpoint().scheme == "unix");
        REQUIRE(n.accepted->remote_endpoint().scheme == "unix");

        std::filesystem::remove(path, rc);
        ++completed;
    }
    REQUIRE(completed == k_iterations);
}
