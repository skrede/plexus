#include "test_recorder_capture_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace recorder_capture_fixture;

TEST_CASE("a recorder destroyed mid-session deregisters before teardown and survives a post-dtor "
          "pump",
          "[recorder_capture][e2e][teardown]")
{
    // The load-bearing teardown-UAF section: build a node + recorder, run a partial session,
    // destroy the recorder (and the node) while the fixture executor stays live, then pump
    // post-dtor. The handle deregisters its tap and drains the ring out in its dtor, so the
    // already-posted self-re-posting drain task finds its block expired and exits without
    // reading a freed ring. asan (-fno-sanitize-recover) is the real gate.
    session_fixture     fx;
    in_memory_byte_sink sink;
    const auto          id = make_id(0x5D);

    {
        plexus::node_options opts;
        inproc_node          n{fx.ex, fx.disc, id, fx.ta, opts};

        {
            auto rec = n.make_recorder(sink);

            // The endpoints live in their own inner scope so their drop edges drain while
            // the node is alive (the facade teardown contract); the recorder captures them.
            {
                plexus::topic_qos qos;
                qos.latch = true;
                plexus::publisher<>  pub{n, "teardown.topic", qos};
                plexus::subscriber<> sub{n, "teardown.topic", [](std::span<const std::byte>, const message_info &) {}};
                for(int i = 0; i < 8; ++i)
                {
                    const std::array<std::byte, 3> mk{std::byte{0x01}, std::byte(i), std::byte{0x02}};
                    pub.publish(mk);
                    fx.drive();
                }
            }
            fx.drive(); // drain the endpoint-drop edges with the node still alive
            // rec destroyed here: deregister-before-teardown + a synchronous drain-out.
        }
        // Pump after the recorder is gone but the node still lives: a stale drain task (if
        // any) must be inert.
        fx.drive();
    }
    // Pump after the node is gone too: the participant-destroy edge uses the snapshot variant
    // and the recorder already deregistered, so no posted closure reads a freed ring.
    fx.drive();

    // The pre-dtor records were drained out; the stream head is recoverable.
    record_stream_reader reader{sink.bytes()};
    stream_definitions   defs;
    REQUIRE(reader.read_definitions(defs));
}
