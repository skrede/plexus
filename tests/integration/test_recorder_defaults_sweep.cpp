// The recorder defaults chosen-cell assertion (the recorded-sweep precedent,
// tests/crypto/test_rekey_threshold_sweep). recorder_sweep measured the recall curve
// recall = min(1, ring_bytes / (burst * framed_record)); this test pins the SHIPPED
// ring_bytes default to its conservative design point: a modest small-payload backlog
// (the common case) is recovered in full at the default budget, while a saturating
// large-payload burst that the default would shed is RECOVERED once ring_bytes is raised
// to a firehose value — proving the override path. A regression that raises the default to
// the firehose ceiling makes the first case no longer the design point and the recovery
// case vacuous. It also asserts the keep-all decimation defaults DO keep everything and
// that the decimation knob — which gates the typed object lane only — decimates exactly as
// configured for a fixed burst. Deterministic: the saturation model is timing-free (a
// publish only POSTS its tap, so a burst queues ahead of the first bounded drain turn) and
// count_n is exact 1/N; no RNG, no single-run claim.

#include "in_memory_byte_sink.h"

#include "plexus/node.h"
#include "plexus/expected.h"
#include "plexus/publisher.h"
#include "plexus/subscriber.h"
#include "plexus/recorder.h"
#include "plexus/wire_bytes.h"
#include "plexus/typed_codec.h"
#include "plexus/node_options.h"
#include "plexus/recording_qos.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/io/message_info.h"
#include "plexus/io/capture_policy.h"
#include "plexus/io/recording/record_envelope.h"
#include "plexus/io/recording/record_stream_reader.h"

#include "plexus/wire/topic_hash.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <memory>
#include <vector>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <system_error>

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_transport;
using plexus::discovery::static_discovery;

using plexus::io::message_info;
using plexus::io::recording::record_category;
using plexus::io::recording::decoded_record;
using plexus::io::recording::recovery_result;
using plexus::io::recording::stream_definitions;
using plexus::io::recording::record_stream_reader;

namespace {

using inproc_node = plexus::node<inproc_policy, inproc_transport<>>;

// A fixed-payload typed value + codec for the decimation cells. The decimation gate
// (capture_policy::wants_payload) governs the TYPED OBJECT lane only — a bytes publish
// carries full payload unconditionally — so the decimation assertion publishes typed
// objects so the gate is exercised. decode is a no-op (the recorder is serializer-agnostic;
// the assertion counts payload-bearing vs envelope-only records).
struct sample
{
    std::uint32_t value{};
};

struct fixed_codec
{
    using value_type = sample;

    std::size_t payload_bytes{256};

    plexus::wire_bytes<> encode(const sample &) const
    {
        auto owner = std::make_shared<std::vector<std::byte>>(payload_bytes, std::byte{0xC3});
        std::span<const std::byte> view{owner->data(), owner->size()};
        return plexus::wire_bytes<>{view, std::move(owner)};
    }

    plexus::expected<void, std::error_code> decode(std::span<const std::byte>, sample &) const
    {
        return {};
    }

    plexus::type_identity type_info() const { return {0xABCD1234u, "sample"}; }
};

plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

plexus::node_options make_opts(plexus::recording_qos capture)
{
    plexus::node_options opts;
    opts.reconnect   = plexus::io::reconnect_config{std::chrono::milliseconds(50),
                                                    std::chrono::milliseconds(2000), std::nullopt,
                                                    std::nullopt};
    opts.redial_seed = 0x9163u;
    opts.capture     = capture;
    return opts;
}

// The fixture owns the bus/executor/transport/discovery so the node + recorder are built in
// an inner scope over the borrowed substrate (the recorder_capture teardown discipline).
struct net
{
    inproc_bus<>       bus;
    inproc_executor<>  ex{bus};
    inproc_transport<> ta{ex, bus};
    static_discovery   disc{{}};

    void drive() { ex.drain(); }
};

plexus::recording_qos keep_all_payload(plexus::io::decimation_mode mode)
{
    plexus::recording_qos cap{};
    cap.fidelity   = plexus::io::capture_fidelity::payload;
    cap.mode       = mode;
    cap.decimation = 1;
    cap.window_ns  = 0;
    return cap;
}

// One saturating-burst run at a given ring budget + payload size. The whole burst is
// PUBLISHED first (each publish only POSTS its capture tap, no synchronous push), so the
// burst's tap closures queue ahead of the recorder's first drain turn; a single drain-to-
// quiescence then runs every push into the byte_ring before the bounded drain ships any
// bytes out — the producer-faster-than-drain regime. recall = recovered samples / burst.
std::uint64_t recovered_samples(std::size_t ring_bytes, std::size_t payload_bytes, int burst)
{
    net        n;
    const auto id = make_id(0x4C);

    in_memory_byte_sink sink;
    {
        inproc_node n_node{n.ex, n.disc, id, n.ta,
                           make_opts(keep_all_payload(plexus::io::decimation_mode::count_n))};

        plexus::recorder_options ro;
        ro.ring_bytes = ring_bytes;
        auto rec      = n_node.make_recorder(sink, std::move(ro));

        {
            plexus::topic_qos qos;
            qos.latch = true;
            plexus::publisher<>  pub{n_node, "defaults.topic", qos};
            plexus::subscriber<> sub{n_node, "defaults.topic",
                                     [](std::span<const std::byte>, const message_info &) {}};
            n.drive();

            const std::vector<std::byte> body(payload_bytes, std::byte{0x5A});
            for(int i = 0; i < burst; ++i)
                pub.publish(body);
            n.drive();
        }
        n.drive();
        rec.flush();
    }
    n.drive();

    record_stream_reader reader{sink.bytes()};
    stream_definitions   defs;
    reader.read_definitions(defs);

    std::vector<decoded_record> records;
    const recovery_result       res = reader.recover(records);
    (void)res;

    std::uint64_t delivered = 0;
    for(const auto &r : records)
        if(r.category == record_category::sample)
            ++delivered;
    return delivered;
}

struct decim_counts
{
    std::uint64_t total{};   // every produced sample record (envelope or payload)
    std::uint64_t payload{}; // sample records that carried real bytes (non-decimated)
};

// One count_n decimation run on the TYPED OBJECT lane (the only path wants_payload gates):
// a fixed input burst at payload fidelity, drained to quiescence with a roomy ring (no
// overflow), so the only thing varying payload-bearing-vs-total is the decimation rule.
decim_counts count_n_run(std::uint32_t n_keep, std::size_t payload_bytes, int burst)
{
    net        n;
    const auto id = make_id(0x5D);

    plexus::recording_qos cap{};
    cap.fidelity   = plexus::io::capture_fidelity::payload;
    cap.mode       = plexus::io::decimation_mode::count_n;
    cap.decimation = n_keep;

    in_memory_byte_sink sink;
    {
        inproc_node n_node{n.ex, n.disc, id, n.ta, make_opts(cap)};

        plexus::recorder_options ro;
        ro.ring_bytes = 1u << 24; // roomy: isolate the decimation effect from ring overflow
        auto rec      = n_node.make_recorder(sink, std::move(ro));

        {
            plexus::typed_publisher_options popts;
            popts.qos.latch  = true;
            popts.pool_depth = 8;
            plexus::publisher<fixed_codec>  pub{n_node, "defaults.topic", popts,
                                                fixed_codec{payload_bytes}};
            plexus::subscriber<fixed_codec> sub{n_node, "defaults.topic", [](const sample &) {}};
            n.drive();

            for(int i = 0; i < burst; ++i)
            {
                auto loan = pub.borrow();
                if(loan)
                {
                    loan->value = static_cast<std::uint32_t>(i);
                    pub.publish(std::move(loan));
                }
                else
                {
                    pub.publish(sample{static_cast<std::uint32_t>(i)});
                }
                n.drive();
            }
        }
        n.drive();
        rec.flush();
    }
    n.drive();

    record_stream_reader reader{sink.bytes()};
    stream_definitions   defs;
    reader.read_definitions(defs);

    std::vector<decoded_record> records;
    reader.recover(records);

    decim_counts c;
    for(const auto &r : records)
    {
        if(r.category != record_category::sample)
            continue;
        ++c.total;
        if(!r.payload.empty())
            ++c.payload;
    }
    return c;
}

// The default's design point: a modest small-payload backlog that fits the 1 MiB ring at
// full recall. 800 records of 512 B is ~400 KiB of raw payload (well inside the framed
// budget) — the common pub/sub small-payload case the conservative default targets.
constexpr int         k_modest_burst   = 800;
constexpr std::size_t k_modest_payload = 512;

// A genuine saturating large-payload burst: 2000 records of 4 KiB is ~8 MiB of raw payload,
// far past the 1 MiB default (so it sheds) yet inside a 16 MiB firehose override (so it
// recovers in full) — the override path.
constexpr int         k_firehose_burst   = 2000;
constexpr std::size_t k_firehose_payload = 4096;
constexpr std::size_t k_firehose_ring    = 16u * 1024u * 1024u;

}

TEST_CASE("recorder defaults the shipped ring_bytes gives full recall for a modest small-payload "
          "workload",
          "[recorder_defaults][sweep]")
{
    // The chosen cell is the SHIPPED default, not a literal. The conservative 1 MiB default
    // is designed for the common small-payload case: a modest backlog of ~512 B records is
    // recovered in full. The saturation model is timing-free, so recall is identical across
    // runs (the recorded-grid reproducibility).
    REQUIRE(plexus::recorder_options{}.ring_bytes == (1u << 20));
    const std::size_t shipped_ring = plexus::recorder_options{}.ring_bytes;

    for(int run = 0; run < 3; ++run)
    {
        const std::uint64_t delivered =
                recovered_samples(shipped_ring, k_modest_payload, k_modest_burst);
        // Full recall: every produced sample of the modest small-payload backlog is
        // recovered at the conservative default — the design point.
        REQUIRE(delivered == static_cast<std::uint64_t>(k_modest_burst));
    }
}

TEST_CASE("recorder defaults raising ring_bytes recovers a saturating burst the default sheds",
          "[recorder_defaults][sweep]")
{
    // The default is conservative on purpose: a saturating large-payload burst (~8 MiB of
    // raw payload) overflows the 1 MiB ring and sheds — observable while recording, the
    // intended posture. This proves the default is a live floor, not a roomy ceiling.
    const std::uint64_t at_default = recovered_samples(plexus::recorder_options{}.ring_bytes,
                                                       k_firehose_payload, k_firehose_burst);
    REQUIRE(at_default < static_cast<std::uint64_t>(k_firehose_burst));

    // Raising ring_bytes to a firehose value recovers the same burst in full — the explicit
    // override path for a large-payload capture.
    const std::uint64_t at_firehose =
            recovered_samples(k_firehose_ring, k_firehose_payload, k_firehose_burst);
    REQUIRE(at_firehose == static_cast<std::uint64_t>(k_firehose_burst));
}

TEST_CASE("recorder defaults the keep-all decimation default keeps every payload",
          "[recorder_defaults][sweep]")
{
    // The shipped decimation default (count_n N = 1) drops nothing: every sample on the
    // typed object lane carries its payload. The consumer-sovereign keep-all posture.
    REQUIRE(plexus::recording_qos{}.decimation == 1u);
    REQUIRE(plexus::recording_qos{}.window_ns == 0u);

    const decim_counts c = count_n_run(/*N=*/1, 256, 400);
    REQUIRE(c.total > 0);
    REQUIRE(c.payload == c.total); // keep-all: every record is payload-bearing
}

TEST_CASE("recorder defaults count_n decimation keeps exactly one of every N on the object lane",
          "[recorder_defaults][sweep]")
{
    // The decimation gate is a typed-object-lane mechanism (wants_payload is consulted on
    // the zero-serialization object path); the assertion drives that lane. count_n is exact
    // and deterministic, so 1-of-N holds with no tolerance.
    const int burst = 400;
    for(std::uint32_t keep : {2u, 4u, 8u})
    {
        const decim_counts c = count_n_run(keep, 256, burst);
        REQUIRE(c.total == static_cast<std::uint64_t>(burst));
        REQUIRE(c.payload == static_cast<std::uint64_t>(burst) / keep);
    }
}
