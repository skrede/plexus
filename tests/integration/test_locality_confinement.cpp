#include "test_locality_confinement_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace locality_confinement_fixture;

TEST_CASE("locality confinement: the synthetic inproc scheme is anchored to the production process "
          "tier",
          "[integration][locality][confinement]")
{
    // Without this anchor the process-tier confinement could pass for the wrong reason
    // if the synthetic scheme ever diverged from the classifier the production path uses.
    REQUIRE(tier_of("inproc") == locality::process);
    REQUIRE(tier_of("unix") == locality::local);
    REQUIRE(tier_of("tcp") == locality::remote);
}

TEST_CASE("locality confinement: the fan-out gate delivers a topic only to its in-mask tiers (full "
          "matrix), looped",
          "[integration][locality][confinement]")
{
    using forwarder = plexus::io::message_forwarder<tagged_policy>;

    constexpr int     k_iterations = 100;
    const std::string fqn          = "demo.confined.topic";
    const std::string payload      = "confined-bytes";

    // The realistic subscriber set: a node fans toward WIRE peers only — a same-host
    // AF_UNIX peer (local tier) and an off-host TCP peer (remote tier). No process-tier
    // subscriber exists, because the process tier currently ships as the BIT +
    // CONFINEMENT only (no in-process positive-delivery sink yet) — so a process-confined
    // topic reaches NO channel here, the pure-isolation property.
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        tagged_channel local_ch{"unix"};
        tagged_channel remote_ch{"tcp"};

        plexus::log::null_logger sink;
        forwarder                fwd{sink};
        fwd.attach(forwarder::peer{local_ch, "local-node"}, fqn);
        fwd.attach(forwarder::peer{remote_ch, "remote-node"}, fqn);

        // process-only: reaches NO wire channel (process ships as confinement-only).
        fwd.declare(fqn, plexus::topic_qos{.reach = locality::process});
        const auto l0 = local_ch.sends, r0 = remote_ch.sends;
        fwd.publish(fqn, as_bytes(payload));
        REQUIRE(local_ch.sends - l0 == 0); // a process-only topic touches no off-process transport
        REQUIRE(remote_ch.sends - r0 == 0);

        // process|local: reaches the local channel, NEVER the remote one.
        fwd.declare(fqn, plexus::topic_qos{.reach = locality::process | locality::local});
        const auto l1 = local_ch.sends, r1 = remote_ch.sends;
        fwd.publish(fqn, as_bytes(payload));
        REQUIRE(local_ch.sends - l1 == 1);  // the local AF_UNIX-tier peer receives
        REQUIRE(remote_ch.sends - r1 == 0); // process|local NEVER goes remote

        // remote-only: reaches only the remote channel.
        fwd.declare(fqn, plexus::topic_qos{.reach = locality::remote});
        const auto l2 = local_ch.sends, r2 = remote_ch.sends;
        fwd.publish(fqn, as_bytes(payload));
        REQUIRE(local_ch.sends - l2 == 0); // a local peer is NEVER reached by a remote topic
        REQUIRE(remote_ch.sends - r2 == 1);

        // any (default): reaches all wire channels.
        fwd.declare(fqn, plexus::topic_qos{.reach = locality::any});
        const auto l3 = local_ch.sends, r3 = remote_ch.sends;
        fwd.publish(fqn, as_bytes(payload));
        REQUIRE(local_ch.sends - l3 == 1);
        REQUIRE(remote_ch.sends - r3 == 1);

        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

// ---- The demand-side gate (engine-driven), deterministic over the inproc bus ----
