#include "test_recorder_capture_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace recorder_capture_fixture;

TEST_CASE("a public-API recorder captures a live multi-endpoint inproc session and recovers it", "[recorder_capture][e2e]")
{
    // This section exercises the runtime behavior of node.make_recorder + the recorder RAII
    // handle: public-API attach -> live multi-topic capture -> cooperative drain on the
    // node's own turns -> offline recovery -> dropout accounting closed.
    for(int run = 0; run < 3; ++run)
    {
        session_fixture fx;
        const auto id = make_id(0x4C);

        in_memory_byte_sink sink;
        const int per_topic = 16;
        std::vector<std::byte> recovered_samples;

        {
            plexus::node_options opts;
            inproc_node n{fx.ex, fx.disc, id, fx.ta, opts};

            // Attach through the PUBLIC verb — no router(), no internal recording type in
            // the attach path. A roomy ring so the full session is captured (recovered ==
            // produced, no dropouts) this run.
            plexus::recorder_options ro;
            ro.ring_bytes = 1u << 20;
            auto rec      = n.make_recorder(sink, std::move(ro));

            // The endpoints live in an INNER scope so their declaration-drop edges (posted
            // capturing the engine `this`) are drained while the node is still alive — the
            // node-facade teardown contract. The recorder + node outlive this scope.
            {
                plexus::topic_qos qos;
                qos.latch = true;
                plexus::publisher<> pub_a{n, "topic.a", qos};
                plexus::publisher<> pub_b{n, "topic.b", qos};
                plexus::subscriber<> sub_a{n, "topic.a", [](std::span<const std::byte>, const message_info &) {}};
                plexus::subscriber<> sub_b{n, "topic.b", [](std::span<const std::byte>, const message_info &) {}};
                fx.drive();

                for(int i = 0; i < per_topic; ++i)
                {
                    const std::array<std::byte, 4> mk{std::byte(0xA0), std::byte(i & 0xff), std::byte{0xBE}, std::byte{0xEF}};
                    pub_a.publish(mk);
                    pub_b.publish(mk);
                    fx.drive(); // the published tap posts; the cooperative drain ships to sink
                }
                fx.drive();
            }
            // The endpoint-drop edges posted at the inner-scope exit are drained here with
            // the node still alive; the recorder captures them too.
            fx.drive();
            rec.flush();
            // rec destroyed here (deregister-before-teardown), then the node dies below.
        }
        // The node + recorder are gone; the fixture executor outlives them. Pump post-dtor:
        // the participant-teardown edge uses the engine's snapshot variant and the recorder
        // already deregistered, so no posted closure touches a freed ring (asan is the gate).
        fx.drive();

        // Recover the captured stream offline (no codec in the path).
        record_stream_reader reader{sink.bytes()};
        stream_definitions defs;
        REQUIRE(reader.read_definitions(defs));
        REQUIRE(defs.node == id);

        std::vector<decoded_record> records;
        const recovery_result res = reader.recover(records);
        REQUIRE(res.header_ok);

        std::uint64_t produced = 0;
        std::uint64_t dropped  = 0;
        std::size_t samples    = 0;
        bool saw_topic_a       = false;
        bool saw_topic_b       = false;
        for(const auto &r : records)
        {
            if(r.category == record_category::dropout)
                dropped += r.count;
            if(r.category != record_category::sample)
                continue;
            ++samples;
            if(r.topic_hash == plexus::wire::fqn_topic_hash("topic.a"))
                saw_topic_a = true;
            if(r.topic_hash == plexus::wire::fqn_topic_hash("topic.b"))
                saw_topic_b = true;
            // The framed sample's tail is the published marker (decoded offline, no tap codec).
            const std::array<std::byte, 2> tail{std::byte{0xBE}, std::byte{0xEF}};
            REQUIRE(payload_ends_with(r.payload, tail));
        }

        // Both topics captured; every produced sample present (roomy ring => zero dropouts).
        REQUIRE(saw_topic_a);
        REQUIRE(saw_topic_b);
        REQUIRE(dropped == 0);
        REQUIRE(samples == static_cast<std::size_t>(2 * per_topic));

        // The dropout accounting closes exactly: delivered + dropped == produced.
        produced = static_cast<std::uint64_t>(2 * per_topic);
        REQUIRE(static_cast<std::uint64_t>(samples) + dropped == produced);
        (void)recovered_samples;
    }
}
