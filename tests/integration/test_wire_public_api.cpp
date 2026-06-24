#include "test_wire_public_api_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace wire_public_api_fixture;

TEST_CASE("public API: a node that opts a transport into wire capture records wire frames", "[wire_public_api][wire]")
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    static_discovery disc{{}};

    // The consumer (plain) and the producer (wire-capturing). The producer's node_options
    // DECLARES the wire opt-in (a designated-initializer aggregate field, consumer-sovereign,
    // no setter) and is composed over the wire-capturing policy + transport, which fixes the
    // decorated channel TYPE at its construction.
    inproc_transport<> consumer_tp{ex, bus};
    inproc_transport<> producer_inner{ex, bus};
    wire_transport producer_tp{producer_inner};

    plexus::node_options consumer_opts = base_opts(/*eager=*/true);
    plexus::node_options producer_opts = base_opts(/*eager=*/true);
    producer_opts.wire                 = plexus::wire_capture_qos{.enabled = true, .position = plexus::wire_crypto_position::cleartext};

    bare_node consumer{ex, disc, make_id(0x0A), consumer_tp, consumer_opts};
    wire_node producer{ex, disc, make_id(0x0B), producer_tp, producer_opts};

    in_memory_byte_sink sink;
    auto recorder = producer.make_recorder(sink);

    consumer.listen({"inproc", "host-a:5000"});
    producer.listen({"inproc", "host-b:6000"});
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

    // The producer's recorder captured framed bytes off the live decorated channel: a node
    // that opted a transport into wire capture produces wire_frame records.
    REQUIRE(count_wire_frames(sink.bytes()) > 0);
}
