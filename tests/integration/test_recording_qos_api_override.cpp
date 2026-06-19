#include "test_recording_qos_api_common.h"

#include <catch2/catch_test_macros.hpp>

using namespace recording_qos_fixture;

TEST_CASE("integration.recording_qos a per-topic publisher override raises capture above an off "
          "node default",
          "[integration][inproc][recording]")
{
    net n{base_opts(/*eager=*/false)}; // node default off: an unoverridden topic stays inert
    n.connect();

    // The selected topic carries a per-topic payload override; the bystander topic relies on
    // the (off) node default. Only the overridden topic encodes.
    counting_codec selected_codec;
    counting_codec bystander_codec;
    auto           selected_encodes  = selected_codec.encodes;
    auto           bystander_encodes = bystander_codec.encodes;

    typed_subscriber sub_sel{n.a, "selected", [](const sample &) {}};
    typed_subscriber sub_by{n.a, "bystander", [](const sample &) {}};

    plexus::typed_publisher_options sel_opts;
    sel_opts.capture = plexus::recording_qos{.fidelity = plexus::io::capture_fidelity::payload};
    typed_publisher pub_sel{n.b, "selected", sel_opts, selected_codec};
    typed_publisher pub_by{n.b, "bystander", plexus::typed_publisher_options{}, bystander_codec};
    n.drive();

    constexpr int k = 4;
    publish_k(pub_sel, n, k);
    publish_k(pub_by, n, k);

    REQUIRE(selected_encodes->load() == k);  // the per-topic override took effect
    REQUIRE(bystander_encodes->load() == 0); // the off node default left the bystander inert
}
