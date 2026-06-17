// Capture a live multi-endpoint session and convert it to an MCAP container that opens in
// Foxglove. This is the analysis path: plexus records to its own flat record stream (the
// canonical, crash-recoverable, MCU-affordable store), and the host-side transcode turns that
// flat stream into MCAP for plotting / scrubbing / CSV export. Single process, public API
// only, self-terminating (no backends, no mDNS).
//
// ── Build ──────────────────────────────────────────────────────────────────────────────────
//   The MCAP transcode is a host-side optional dependency (the `mcap` library is NOT in the
//   header-only core and NOT on the MCU). Enable it explicitly:
//
//     cmake -B build -DPLEXUS_BUILD_EXAMPLES=ON -DPLEXUS_BUILD_MCAP_TRANSCODE=ON
//     cmake --build build -j4 --target mcap_example
//
//   (Without -DPLEXUS_BUILD_MCAP_TRANSCODE=ON this example is not built — see recording_example
//    for the capture-only path that builds unconditionally.)
//
// ── Run ────────────────────────────────────────────────────────────────────────────────────
//     ./build/examples/mcap_example
//
//   It writes two files in the working directory:
//     mcap_example_capture.plxr   the flat record stream (the canonical capture)
//     mcap_example_capture.mcap   the MCAP container (the analysis artifact)
//   and prints the transcode summary (schemas / channels / messages + recovery accounting).
//
// ── Open / read / use the MCAP output ──────────────────────────────────────────────────────
//   Foxglove (the intended consumer):
//     • Desktop app or the web app at https://studio.foxglove.dev
//     • File ▸ Open local file…  (or drag mcap_example_capture.mcap onto the window)
//     • Plot any numeric field over time, scrub the timeline, or export a panel to CSV.
//   The `mcap` CLI (https://github.com/foxglove/mcap — `mcap` command) for a quick look:
//     mcap info mcap_example_capture.mcap          # summary: channels, schemas, message counts
//     mcap list channels mcap_example_capture.mcap # the channel ▸ schema mapping
//     mcap cat mcap_example_capture.mcap --json    # every message as JSON
//
//   What the channels are: each captured data topic becomes its own channel keyed by the topic
//   name + type id (the raw payload bytes are laid in verbatim and the encoding is named in the
//   schema — no plexus codec runs, the store is serializer-agnostic). Control-plane events
//   (endpoint/QoS/drop/security lifecycle) ride synthetic `plexus/events/*` channels; captured
//   wire frames ride a `plexus/wire` channel.
//
// ── Transcode an existing capture ─────────────────────────────────────────────────────────
//   The same conversion is available as a standalone CLI for any .plxr produced anywhere
//   (including a stream collected off an MCU and copied to the host):
//     mcap_transcode <in.plxr> <out.mcap>

#include "plexus/node.h"
#include "plexus/expected.h"
#include "plexus/publisher.h"
#include "plexus/recorder.h"
#include "plexus/wire_bytes.h"
#include "plexus/subscriber.h"
#include "plexus/typed_codec.h"
#include "plexus/node_options.h"
#include "plexus/recording_qos.h"
#include "plexus/tools/flat_to_mcap.h"

#include "plexus/io/message_info.h"
#include "plexus/io/recording/byte_sink.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/discovery/static_discovery.h"

#include <span>
#include <vector>
#include <memory>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <filesystem>

using plexus::inproc::inproc_policy;
using transport_t = plexus::inproc::inproc_transport<>;
using node_t      = plexus::node<inproc_policy, transport_t>;

struct reading
{
    std::uint32_t sensor{};
    std::uint32_t value{};
};

struct reading_codec
{
    using value_type = reading;

    plexus::wire_bytes<> encode(const reading &r) const
    {
        auto owner = std::make_shared<std::vector<std::byte>>(8);
        for(int i = 0; i < 4; ++i)
        {
            (*owner)[i]     = static_cast<std::byte>((r.sensor >> (8 * i)) & 0xff);
            (*owner)[4 + i] = static_cast<std::byte>((r.value >> (8 * i)) & 0xff);
        }
        return {std::span<const std::byte>{owner->data(), owner->size()}, std::move(owner)};
    }

    plexus::expected<void, std::error_code> decode(std::span<const std::byte> b, reading &out) const
    {
        if(b.size() != 8)
            return plexus::expected<void, std::error_code>{
                plexus::unexpect, std::make_error_code(std::errc::invalid_argument)};
        auto u32 = [&](int o) {
            std::uint32_t v = 0;
            for(int i = 0; i < 4; ++i)
                v |= static_cast<std::uint32_t>(std::to_integer<std::uint8_t>(b[o + i])) << (8 * i);
            return v;
        };
        out = {u32(0), u32(4)};
        return {};
    }

    plexus::type_identity type_info() const { return {0x5E4501u, "reading"}; }
};

// A consumer-owned drain target over the public byte_sink seam: it accumulates the drained flat
// stream in memory so the example can write it to a file and hand it to the transcode. A real
// binding would drain straight to a file / serial / SD card — the seam is a raw byte drain.
class buffer_sink final : public plexus::io::recording::byte_sink
{
public:
    void write(std::span<const std::byte> bytes) override
    {
        m_bytes.insert(m_bytes.end(), bytes.begin(), bytes.end());
    }

    std::span<const std::byte> bytes() const noexcept { return m_bytes; }

private:
    std::vector<std::byte> m_bytes;
};

plexus::node_options opts(std::uint64_t seed, bool eager, plexus::recording_qos capture)
{
    plexus::node_options o;
    o.redial_seed  = seed;
    o.dial_eagerly = eager;
    o.capture      = capture;
    return o;
}

plexus::node_id id_of(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

int main()
{
    // Capture each selected topic's payload, keeping every sample (the keep-all default).
    plexus::recording_qos capture{};
    capture.fidelity = plexus::io::capture_fidelity::payload;

    plexus::inproc::inproc_bus<>        bus;
    plexus::inproc::inproc_executor<>   ex{bus};
    transport_t                         ta{ex, bus};
    transport_t                         tb{ex, bus};
    plexus::discovery::static_discovery disc{{}};

    buffer_sink sink;

    {
        node_t pub_node{ex, disc, id_of(0x0A), ta, opts(0xA, /*eager=*/false, capture)};
        node_t sub_node{ex, disc, id_of(0x0B), tb, opts(0xB, /*eager=*/true, capture)};
        sub_node.listen({"inproc", "host-b:6000"});
        pub_node.listen({"inproc", "host-a:5000"});
        ex.drain();

        auto rec = pub_node.make_recorder(sink);

        {
            plexus::publisher<reading_codec> temp{
                pub_node, "telemetry.temperature", plexus::typed_publisher_options{}, reading_codec{}};
            plexus::publisher<reading_codec> press{
                pub_node, "telemetry.pressure", plexus::typed_publisher_options{}, reading_codec{}};
            plexus::subscriber<reading_codec> temp_sub{
                sub_node, "telemetry.temperature", [](const reading &) {}};
            plexus::subscriber<reading_codec> press_sub{
                sub_node, "telemetry.pressure", [](const reading &) {}};
            ex.drain();

            for(std::uint32_t i = 0; i < 8; ++i)
            {
                temp.publish(reading{1, 200 + i});
                press.publish(reading{2, 1000 + i});
                ex.drain();
            }
            ex.drain();
        }
        ex.drain();
        rec.flush();
    }
    ex.drain();

    const std::filesystem::path flat_path = "mcap_example_capture.plxr";
    {
        std::ofstream out{flat_path, std::ios::binary};
        out.write(reinterpret_cast<const char *>(sink.bytes().data()),
                  static_cast<std::streamsize>(sink.bytes().size()));
    }
    std::cout << "captured " << sink.bytes().size() << " bytes -> " << flat_path.string() << '\n';

    const std::filesystem::path mcap_path = "mcap_example_capture.mcap";
    const auto result = plexus::tools::flat_to_mcap(sink.bytes(), mcap_path);
    if(!result.ok)
    {
        std::cout << "transcode failed: " << result.error << '\n';
        return 1;
    }

    std::cout << "transcoded -> " << mcap_path.string() << ": " << result.schemas << " schemas, "
              << result.channels << " channels, " << result.messages << " messages ("
              << result.recovered << " records recovered)\n";
    std::cout << "open it in Foxglove (https://studio.foxglove.dev), or inspect: mcap info "
              << mcap_path.string() << '\n';
    return 0;
}
