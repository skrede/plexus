#include "test_reliability_enforcement_common.h"

using namespace reliability_enforcement_fixture;

TEST_CASE("reliability enforcement: the permissive default admits a best_effort 'udp' peer",
          "[udp][enforcement][permissive]")
{
    rendezvous r{"udp"};
    r.dialer.subscribe(r.peer, "topic/x"); // no requirement -> permissive default
    REQUIRE(r.connected());                // admitted: reach -> dial -> session
}

TEST_CASE("reliability enforcement: a strict-reliable demand toward a best_effort 'udp' peer is "
          "refused pre-dial",
          "[udp][enforcement][strict]")
{
    rendezvous r{"udp"};
    r.dialer.subscribe(r.peer, "topic/x", locality::any, reliability_requirement::reliable);
    REQUIRE_FALSE(r.connected()); // refused: NO slot, no demand, no dial
}

TEST_CASE("reliability enforcement: a strict-reliable demand toward a 'udpr' reliable-datagram "
          "peer is admitted",
          "[udp][enforcement][strict]")
{
    rendezvous r{"udpr"}; // the reliable-datagram opt-in: a reliable class
    r.dialer.subscribe(r.peer, "topic/x", locality::any, reliability_requirement::reliable);
    REQUIRE(r.connected()); // admitted: udpr satisfies reliable
}

TEST_CASE("reliability enforcement: a strict-reliable demand toward a 'tcp' peer is admitted",
          "[udp][enforcement][strict]")
{
    rendezvous r{"tcp"};
    r.dialer.subscribe(r.peer, "topic/x", locality::any, reliability_requirement::reliable);
    REQUIRE(r.connected()); // admitted: tcp is a reliable stream
}

TEST_CASE("reliability enforcement: a strict-reliable demand toward an UNKNOWN peer fails closed",
          "[udp][enforcement][strict]")
{
    rendezvous r{"tcp"}; // responder listens, but the dialer forgets it
    r.dialer.subscribe(make_id(0xCC), "topic/x", locality::any, reliability_requirement::reliable);
    r.drive();
    REQUIRE_FALSE(r.dialer.has_session(make_id(0xCC))); // fail-closed: unknown peer refused
}
