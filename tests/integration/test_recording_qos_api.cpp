#include "test_recording_qos_api_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace recording_qos_fixture;

TEST_CASE("integration.recording_qos a node declaring no recording QoS ships zero capture",
          "[integration][inproc][recording]")
{
    net n{base_opts(/*eager=*/false)}; // producer leaves node_options.capture at the off default
    n.connect();

    counting_codec   codec;
    auto             encodes = codec.encodes;
    typed_subscriber sub{n.a, "topic", [](const sample &) {}};
    typed_publisher  pub{n.b, "topic", plexus::typed_publisher_options{}, codec};
    n.drive();

    publish_k(pub, n, 5);
    REQUIRE(encodes->load() == 0); // the off default selects nothing: the gate stays inert
}

TEST_CASE("integration.recording_qos a node-level payload default fires the encode per publish",
          "[integration][inproc][recording]")
{
    plexus::node_options producer = base_opts(/*eager=*/false);
    producer.capture = plexus::recording_qos{.fidelity = plexus::io::capture_fidelity::payload};
    net n{producer};
    n.connect();

    counting_codec   codec;
    auto             encodes = codec.encodes;
    typed_subscriber sub{n.a, "topic", [](const sample &) {}};
    typed_publisher  pub{n.b, "topic", plexus::typed_publisher_options{}, codec};
    n.drive();

    constexpr int k = 5;
    publish_k(pub, n, k);
    REQUIRE(encodes->load() == k); // the node default selects every topic for payload capture
}
