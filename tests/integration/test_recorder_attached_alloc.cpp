// The recorder-attached alloc gate: with a recorder draining, the steady-state push ->
// encode -> ring -> drain path must allocate ZERO after attach. The ring is grown ONCE at
// construction and the encoder's scratch is reused across records, so a warmed saturating
// producer touches no heap on the recorder's own write/drain path. The drain target is a
// pre-grown fixed-capacity sink that overwrites in place (so the measurement isolates the
// recorder machinery, not the destination's growth). A contrast cell shows the inert
// baseline — no recorder, no observer — is itself zero on the same loop, so attaching a
// recorder adds zero steady-state allocation.
//
// The replaceable global new/delete (support/alloc_counter.h) constrains this to ONE TU per
// executable, so it is its own ctest binary. The gate loops >=3x (medians).

#include "support/alloc_counter.h"

#include "plexus/io/message_info.h"
#include "plexus/io/capture_policy.h"
#include "plexus/io/recording/byte_sink.h"
#include "plexus/io/recording/wire_record.h"
#include "plexus/io/recording/flat_recorder.h"

#include "plexus/node_id.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <algorithm>

using plexus::node_id;
using plexus::io::message_info;
using plexus::io::capture_fidelity;
using plexus::io::recording::byte_sink;
using plexus::io::recording::wire_direction;
using plexus::io::recording::flat_recorder;

namespace {

// A pre-grown fixed-capacity drain target: it reserves once and overwrites a ring cursor in
// place, so a steady-state write never reallocates. It is NOT a recorder primitive — only
// the test's destination, chosen so the measurement isolates the recorder's own path from
// the sink's growth (the in-memory test sink grows; that growth is not what this gate
// measures).
class fixed_capacity_sink final : public byte_sink
{
public:
    explicit fixed_capacity_sink(std::size_t capacity)
            : m_buf(capacity)
            , m_at(0)
    {
    }

    void write(std::span<const std::byte> bytes) override
    {
        for(std::byte b : bytes)
        {
            m_buf[m_at] = b;
            if(++m_at == m_buf.size())
                m_at = 0; // wrap: a sink that never grows, so the gate measures only the recorder
        }
    }

private:
    std::vector<std::byte> m_buf;
    std::size_t            m_at;
};

std::uint64_t monotonic_clock()
{
    static std::uint64_t t = 0;
    return ++t;
}

}

TEST_CASE("recorder-attached steady-state push/encode/drain allocates zero after attach",
          "[integration]")
{
    constexpr int     warm = 256;
    constexpr int     K    = 8192;
    const std::size_t ring = 1u << 20;

    const std::vector<std::byte> body(64, std::byte{0x5A});

    for(int run = 0; run < 3; ++run)
    {
        fixed_capacity_sink sink{1u << 20};
        flat_recorder       rec{sink, ring, [] { return monotonic_clock(); }};

        auto push = [&](std::uint64_t i)
        {
            message_info info{};
            info.publication_sequence = i;
            rec.record_sample(0x1234, info, 0, false, capture_fidelity::payload, body);
            rec.pump(); // drain on the same turn (the cooperative discipline), into the pre-grown
                        // sink
        };

        // Warm: grow the ring backing store + the encoder scratch (both grown ONCE here).
        for(int i = 0; i < warm; ++i)
            push(static_cast<std::uint64_t>(i));

        plexus::testing::reset_alloc_count();
        const auto before = plexus::testing::alloc_count();
        for(int i = 0; i < K; ++i)
            push(0xF0000000ull + static_cast<std::uint64_t>(i));
        const auto after = plexus::testing::alloc_count();

        REQUIRE(after - before == 0); // grown-once: the saturating producer + drain touched no heap
    }
}

TEST_CASE("wire-attached steady-state record_wire/drain allocates zero after attach",
          "[integration]")
{
    // The wire tier rides the SAME grown-once ring + reused encoder scratch as every other
    // record, so a warmed saturating record_wire -> ring -> drain loop touches no heap. The
    // decorator's only inherent every-packet cost is the owner-carry copy of the frame into the
    // posted turn, which lives in the engine's post_wire (not measured here); this gate isolates
    // the recorder's own write/drain path with the frame bytes already in hand (the fidelity is
    // capture_fidelity::wire, so an overflow would shed at the wire tier). Mirrors the
    // recorder-attached sample gate exactly. Loops >=3x (medians).
    constexpr int     warm = 256;
    constexpr int     K    = 8192;
    const std::size_t ring = 1u << 20;

    const std::vector<std::byte> frame(128, std::byte{0xC7});
    const node_id                peer{};

    for(int run = 0; run < 3; ++run)
    {
        fixed_capacity_sink sink{1u << 20};
        flat_recorder       rec{sink, ring, [] { return monotonic_clock(); }};

        auto push = [&](std::uint64_t seq)
        {
            rec.record_wire(wire_direction::out, seq, peer, frame);
            rec.pump();
        };

        for(int i = 0; i < warm; ++i)
            push(static_cast<std::uint64_t>(i));

        plexus::testing::reset_alloc_count();
        const auto before = plexus::testing::alloc_count();
        for(int i = 0; i < K; ++i)
            push(0xF0000000ull + static_cast<std::uint64_t>(i));
        const auto after = plexus::testing::alloc_count();

        REQUIRE(after - before ==
                0); // grown-once: the saturating wire producer + drain touched no heap
    }
}

TEST_CASE("the inert baseline (no recorder) is itself zero on the same loop", "[integration]")
{
    // The no-recorder path the recorder-attached gate is measured against: the same body
    // copied into a pre-grown buffer with no recorder in the loop is zero-alloc, so the
    // attached gate's zero is attributable to the recorder being grown-once, not to an empty
    // loop. (The full no-recorder hot-path inert baseline lives in test_hot_path_alloc /
    // test_typed_inproc_alloc; this cell is the local contrast.)
    constexpr int                K = 8192;
    const std::vector<std::byte> body(64, std::byte{0x5A});
    std::vector<std::byte>       buf(body.size());

    for(int i = 0; i < 64; ++i)
        std::copy(body.begin(), body.end(), buf.begin());

    plexus::testing::reset_alloc_count();
    const auto before = plexus::testing::alloc_count();
    for(int i = 0; i < K; ++i)
        std::copy(body.begin(), body.end(), buf.begin());
    const auto after = plexus::testing::alloc_count();

    REQUIRE(after - before == 0);
}
