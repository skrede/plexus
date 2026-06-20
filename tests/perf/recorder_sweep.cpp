// over-limit: one cohesive measurement rig bound to the single-TU alloc_counter; both sweeps
// share the one net/sink harness + median drivers + main report and the program-wide
// operator-new override, so splitting it across TUs scatters that single-TU allocation
// contract and the one measurement program.
// A standalone measurement rig for the recorder's two numeric byte/rate knobs. It drives a
// saturating producer through the PUBLIC node.make_recorder over an in-memory byte_sink and
// reports medians over >=3 runs (it REPORTS numbers, it is NOT a ctest — the parity suite
// count is untouched, mirroring typed_inproc_rig).
//
// The saturation model exploits the cooperative single-thread executor: a publish only
// POSTS the capture tap (no synchronous push); a burst of P publishes therefore queues P
// tap closures ahead of the recorder's first drain turn (the drain re-posts itself to the
// BACK of the same FIFO queue). Draining to quiescence then runs all P tap closures FIRST,
// pushing every record into the byte_ring before any drain ships bytes to the sink — so a
// burst whose framed bytes exceed ring_bytes overflows the ring and sheds, exactly the
// producer-faster-than-drain regime the byte budget bounds. The shed is accounted exactly
// (dropout_record), so recall = delivered / produced is read back offline from the stream.
//
//   Sweep 1 (ring_bytes): recall vs ring budget for a fixed saturating burst, per payload
//           size. The knee is the smallest budget at which recall saturates (~1.0).
//   Sweep 2 (decimation): achieved payload-bearing output rate vs the configured window/N.
//           count_n keeps 1 of every N; time_window pins at most one payload per window. The
//           measured ratio confirms the knob does what it claims for a fixed input burst.

#include "support/alloc_counter.h"

#include "plexus/node.h"
#include "plexus/publisher.h"
#include "plexus/subscriber.h"
#include "plexus/recorder.h"
#include "plexus/expected.h"
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

#include <span>
#include <array>
#include <memory>
#include <vector>
#include <chrono>
#include <cstdio>
#include <cstddef>
#include <cstdint>
#include <system_error>
#include <algorithm>

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_transport;
using plexus::discovery::static_discovery;

using plexus::io::recording::record_category;
using plexus::io::recording::decoded_record;
using plexus::io::recording::recovery_result;
using plexus::io::recording::stream_definitions;
using plexus::io::recording::record_stream_reader;

namespace {

using inproc_node = plexus::node<inproc_policy, inproc_transport<>>;

// A fixed-payload typed value + codec for the decimation sweep. Decimation (wants_payload)
// gates the TYPED OBJECT lane only — the bytes publish path always carries full payload — so
// the decimation cell must publish typed objects to exercise the gate. The codec emits a
// fixed-size buffer so each captured payload is a constant size; decode is a no-op (the
// recorder is serializer-agnostic; the rig only counts payload-bearing vs envelope-only).
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

// The non-disk drain target: it appends every drained record into one growing buffer the
// rig recovers offline. A faithful drop-in for an MCU serial/SD binding — the seam is a raw
// byte_sink, nothing disk-specific.
class growing_byte_sink final : public plexus::io::recording::byte_sink
{
public:
    void write(std::span<const std::byte> bytes) override
    {
        m_bytes.insert(m_bytes.end(), bytes.begin(), bytes.end());
    }
    [[nodiscard]] std::span<const std::byte> bytes() const noexcept { return m_bytes; }

private:
    std::vector<std::byte> m_bytes;
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

// The fixture owns the bus/executor/transport/discovery so the node + recorder can be built
// in an inner scope over the borrowed substrate (the recorder_capture teardown discipline).
struct net
{
    inproc_bus<>       bus;
    inproc_executor<>  ex{bus};
    inproc_transport<> ta{ex, bus};
    static_discovery   disc{{}};

    void drive() { ex.drain(); }
};

struct recall_point
{
    std::uint64_t produced;  // the burst (ground truth: one sample tap per publish)
    std::uint64_t delivered; // sample records recovered from the drained stream
    double        recall;    // delivered / produced
};

// One saturating-burst run at a given ring budget + payload size. The whole burst is
// PUBLISHED first (each publish only POSTS its capture tap, no synchronous push), so the P
// tap closures queue ahead of the recorder's first drain turn; a single drain-to-quiescence
// then runs every tap push into the byte_ring BEFORE the bounded drain ships any bytes out.
// A burst whose framed bytes exceed ring_bytes therefore overflows and sheds — the producer-
// faster-than-drain regime. recall = recovered samples / burst: the burst is the exact
// ground truth (one published-tap sample record per publish, verified), so recall is read
// against it directly rather than against the shed accounting (a saturated ring can shed even
// its own dropout_record, so the recovered shed count is a floor on loss, not the metric).
recall_point run_ring_cell(std::size_t ring_bytes, std::size_t payload_bytes, int burst,
                           plexus::recording_qos capture)
{
    net        n;
    const auto id = make_id(0x4C);

    growing_byte_sink sink;
    {
        inproc_node n_node{n.ex, n.disc, id, n.ta, make_opts(capture)};

        plexus::recorder_options ro;
        ro.ring_bytes = ring_bytes;
        auto rec      = n_node.make_recorder(sink, std::move(ro));

        {
            // latch admits the published tap (a non-latched topic with no remote subscriber
            // short-circuits the publish before the tap fires — the demand-driven send gate).
            plexus::topic_qos qos;
            qos.latch = true;
            plexus::publisher<>  pub{n_node, "sweep.topic", qos};
            plexus::subscriber<> sub{
                    n_node, "sweep.topic",
                    [](std::span<const std::byte>, const plexus::io::message_info &) {}};
            n.drive();

            const std::vector<std::byte> body(payload_bytes, std::byte{0x5A});
            for(int i = 0; i < burst; ++i)
                pub.publish(body); // POST only — the tap push happens on the drain turn
            n.drive();             // run all tap pushes, then the bounded drain to quiescence
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

    const std::uint64_t produced = static_cast<std::uint64_t>(burst);
    const double        recall =
            produced == 0 ? 0.0 : static_cast<double>(delivered) / static_cast<double>(produced);
    return {produced, delivered, recall};
}

struct decim_point
{
    std::uint64_t total_samples;   // every produced sample record (envelope or payload)
    std::uint64_t payload_bearing; // sample records that carried real bytes (non-decimated)
    double        keep_ratio;      // payload_bearing / total_samples
};

// One decimation run on the TYPED OBJECT lane (the only path the wants_payload gate governs):
// a fixed input burst at payload fidelity, drained to quiescence with a roomy ring (no
// overflow), so the only thing that varies payload-bearing-vs-total is the decimation rule.
// Each publish is driven to quiescence so the time_window gate sees real wall-clock spacing
// between records (the source clock is wire::now_timestamp_ns).
decim_point run_decim_cell(plexus::recording_qos capture, std::size_t payload_bytes, int burst)
{
    net        n;
    const auto id = make_id(0x5D);

    growing_byte_sink sink;
    {
        inproc_node n_node{n.ex, n.disc, id, n.ta, make_opts(capture)};

        plexus::recorder_options ro;
        ro.ring_bytes = 1u << 24; // roomy: isolate the decimation effect from ring overflow
        auto rec      = n_node.make_recorder(sink, std::move(ro));

        {
            plexus::typed_publisher_options popts;
            popts.qos.latch  = true;
            popts.pool_depth = 8;
            plexus::publisher<fixed_codec>  pub{n_node, "sweep.topic", popts,
                                                fixed_codec{payload_bytes}};
            plexus::subscriber<fixed_codec> sub{n_node, "sweep.topic", [](const sample &) {}};
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
                n.drive(); // drive each publish so the time_window clock advances per record
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

    std::uint64_t total   = 0;
    std::uint64_t payload = 0;
    for(const auto &r : records)
    {
        if(r.category != record_category::sample)
            continue;
        ++total;
        if(!r.payload.empty())
            ++payload;
    }
    const double keep =
            total == 0 ? 0.0 : static_cast<double>(payload) / static_cast<double>(total);
    return {total, payload, keep};
}

double median_recall(std::size_t ring_bytes, std::size_t payload_bytes, int burst,
                     plexus::recording_qos capture, int runs)
{
    std::vector<double> v;
    v.reserve(static_cast<std::size_t>(runs));
    for(int r = 0; r < runs; ++r)
        v.push_back(run_ring_cell(ring_bytes, payload_bytes, burst, capture).recall);
    std::sort(v.begin(), v.end());
    return v[v.size() / 2];
}

double median_keep(plexus::recording_qos capture, std::size_t payload_bytes, int burst, int runs)
{
    std::vector<double> v;
    v.reserve(static_cast<std::size_t>(runs));
    for(int r = 0; r < runs; ++r)
        v.push_back(run_decim_cell(capture, payload_bytes, burst).keep_ratio);
    std::sort(v.begin(), v.end());
    return v[v.size() / 2];
}

const char *kib(std::size_t b, char *buf, std::size_t n)
{
    if(b >= (1u << 20))
        std::snprintf(buf, n, "%zuMiB", b >> 20);
    else
        std::snprintf(buf, n, "%zuKiB", b >> 10);
    return buf;
}

}

int main(int argc, char **argv)
{
    const int runs  = argc > 1 ? std::atoi(argv[1]) : 5;
    const int burst = argc > 2 ? std::atoi(argv[2]) : 4000;

    std::printf("recorder defaults sweep: runs=%d (median reported), saturating burst=%d "
                "publishes/cell\n",
                runs, burst);
    std::printf("  payload fidelity, no decimation; recall = delivered / produced (exact "
                "dropout accounting). CPU-exclusive run.\n\n");

    // ---- Sweep 1: recall vs ring_bytes, per payload size --------------------------------
    plexus::recording_qos cap_full{};
    cap_full.fidelity   = plexus::io::capture_fidelity::payload;
    cap_full.mode       = plexus::io::decimation_mode::count_n;
    cap_full.decimation = 1; // keep every record (no decimation) for the recall sweep

    const std::size_t rings[]    = {64u * 1024u, 128u * 1024u, 256u * 1024u, 512u * 1024u,
                                    1u << 20,    2u << 20,     4u << 20,     16u << 20};
    const std::size_t payloads[] = {64, 256, 1024, 4096};

    std::printf("  Sweep 1 — recall (median of %d) vs ring_bytes:\n", runs);
    std::printf("    %10s", "ring");
    for(std::size_t p : payloads)
        std::printf("  %10zuB", p);
    std::printf("\n");
    for(std::size_t rb : rings)
    {
        char buf[16];
        std::printf("    %10s", kib(rb, buf, sizeof buf));
        for(std::size_t p : payloads)
        {
            const double m = median_recall(rb, p, burst, cap_full, runs);
            std::printf("  %11.4f", m);
        }
        std::printf("\n");
    }
    std::printf("\n");

    // ---- Sweep 2a: count_n decimation keep-ratio ---------------------------------------
    std::printf("  Sweep 2a — count_n keep-ratio (median of %d), payload=256B, burst=%d:\n", runs,
                burst);
    std::printf("    %8s  %14s  %16s\n", "N", "keep(measured)", "expected(1/N)");
    const std::uint32_t ns[] = {1, 2, 4, 8, 16, 32, 64};
    for(std::uint32_t nn : ns)
    {
        plexus::recording_qos cap{};
        cap.fidelity   = plexus::io::capture_fidelity::payload;
        cap.mode       = plexus::io::decimation_mode::count_n;
        cap.decimation = nn;
        const double m = median_keep(cap, 256, burst, runs);
        std::printf("    %8u  %14.4f  %16.4f\n", nn, m, 1.0 / static_cast<double>(nn));
    }
    std::printf("\n");

    // ---- Sweep 2b: time_window decimation keep-ratio -----------------------------------
    // The source clock is wire::now_timestamp_ns (wall ns); each publish is driven to
    // quiescence so the window gate sees real elapsed time between records.
    std::printf("  Sweep 2b — time_window keep-ratio (median of %d), payload=256B, burst=%d:\n",
                runs, burst);
    std::printf("    %12s  %16s\n", "window", "keep(measured)");
    const std::uint64_t windows_ns[] = {0, 1000, 10000, 100000, 1000000, 10000000};
    for(std::uint64_t w : windows_ns)
    {
        plexus::recording_qos cap{};
        cap.fidelity   = plexus::io::capture_fidelity::payload;
        cap.mode       = plexus::io::decimation_mode::time_window;
        cap.window_ns  = w;
        const double m = median_keep(cap, 256, burst, runs);
        std::printf("    %12llu  %16.4f\n", static_cast<unsigned long long>(w), m);
    }
    std::printf("\n");

    return 0;
}
