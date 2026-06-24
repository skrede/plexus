#include "test_wire_public_api_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace wire_public_api_fixture;

TEST_CASE("public API: a default node records no wire frames (structural absence)", "[wire_public_api][wire]")
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    static_discovery disc{{}};

    inproc_transport<> consumer_tp{ex, bus};
    inproc_transport<> producer_tp{ex, bus};

    // Both nodes default: node_options.wire is left at its disabled default, so each is
    // composed over the bare inproc policy and mints bare channels — the decorator is
    // structurally absent (the compile-time witness above proves the type).
    plexus::node_options consumer_opts = base_opts(/*eager=*/true);
    plexus::node_options producer_opts = base_opts(/*eager=*/true);
    REQUIRE(producer_opts.wire.enabled == false);

    bare_node consumer{ex, disc, make_id(0x1A), consumer_tp, consumer_opts};
    bare_node producer{ex, disc, make_id(0x1B), producer_tp, producer_opts};

    in_memory_byte_sink sink;
    auto recorder = producer.make_recorder(sink);

    consumer.listen({"inproc", "host-c:5000"});
    producer.listen({"inproc", "host-d:6000"});
    ex.drain();

    typed_subscriber sub{consumer, "telemetry", [](const reading &) {}};
    typed_publisher pub{producer, "telemetry", plexus::typed_publisher_options{}, reading_codec{}};
    ex.drain();

    for(int i = 0; i < 8; ++i)
    {
        auto loan = pub.borrow();
        REQUIRE(loan);
        loan->value = static_cast<std::uint32_t>(i);
        pub.publish(std::move(loan));
        ex.drain();
    }
    while(recorder.pump())
        ;
    recorder.flush();

    // No wire opt-in, no decorator composed: the recorder captures the metadata/payload floor
    // but no wire_frame record — the wire tier is structurally absent.
    REQUIRE(count_wire_frames(sink.bytes()) == 0);
}
