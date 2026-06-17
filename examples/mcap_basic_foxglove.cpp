// Capture a live session whose payloads are bare JSON and convert it to an MCAP that plots
// in Foxglove out of the box. plexus records to its own flat record stream (the canonical,
// crash-recoverable, MCU-affordable store); the host-side transcode turns that stream into
// MCAP. The codec emits a small JSON object per sample, and the recorder declares a matching
// jsonschema in the stream preamble — so the transcode labels the data channel "json" with a
// real JSON Schema and Foxglove decodes/plots the numeric fields with no further setup.
// Single process, public API only, self-terminating (no backends, no mDNS).
//
// ── Build ──────────────────────────────────────────────────────────────────────────────────
//   The MCAP transcode is a host-side optional dependency (the `mcap` library is NOT in the
//   header-only core and NOT on the MCU). Enable it explicitly:
//
//     cmake -B build -DPLEXUS_BUILD_EXAMPLES=ON -DPLEXUS_BUILD_MCAP_TRANSCODE=ON
//     cmake --build build -j4 --target mcap_basic_foxglove
//
//   (Without -DPLEXUS_BUILD_MCAP_TRANSCODE=ON this example is not built — see recording_example
//    for the capture-only path that builds unconditionally.)
//
// ── Run ────────────────────────────────────────────────────────────────────────────────────
//     ./build/examples/mcap_basic_foxglove
//
//   It writes two files in the working directory:
//     mcap_basic_foxglove.plxr   the flat record stream (the canonical capture)
//     mcap_basic_foxglove.mcap   the MCAP container (the analysis artifact)
//   and prints the transcode summary (schemas / channels / messages + recovery accounting).
//
// ── Open / read / use the MCAP output ──────────────────────────────────────────────────────
//   Foxglove (the intended consumer):
//     • Desktop app or the web app at https://studio.foxglove.dev
//     • File ▸ Open local file…  (or drag mcap_basic_foxglove.mcap onto the window)
//     • The telemetry channels decode out of the box (their declared encoding is "json" and
//       the schema is a real jsonschema): plot the "value" field over time, scrub, export CSV.
//   The `mcap` CLI (https://github.com/foxglove/mcap — `mcap` command) for a quick look:
//     mcap info mcap_basic_foxglove.mcap          # summary: channels, schemas, message counts
//     mcap list channels mcap_basic_foxglove.mcap # the channel ▸ schema mapping
//     mcap cat mcap_basic_foxglove.mcap --json    # every message as JSON
//
//   The data topics decode because the stored bytes ARE the codec output (bare JSON, the
//   payload-fidelity tier) and the preamble declares the matching json/jsonschema pair. The
//   transcode validates nothing about the bytes — the honesty is the producer's contract: the
//   codec emits exactly what the declared schema describes.

#include "plexus/node.h"
#include "plexus/expected.h"
#include "plexus/publisher.h"
#include "plexus/recorder.h"
#include "plexus/wire_bytes.h"
#include "plexus/subscriber.h"
#include "plexus/type_schema.h"
#include "plexus/typed_codec.h"
#include "plexus/node_options.h"
#include "plexus/recording_qos.h"
#include "plexus/recorder_options.h"
#include "plexus/tools/flat_to_mcap.h"

#include "plexus/io/recording/byte_sink.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/discovery/static_discovery.h"

#include <span>
#include <string>
#include <vector>
#include <memory>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <filesystem>
#include <string_view>

using plexus::inproc::inproc_policy;
using transport_t = plexus::inproc::inproc_transport<>;
using node_t      = plexus::node<inproc_policy, transport_t>;

struct reading
{
    std::uint32_t sensor{};
    std::uint32_t value{};
};

// Emits a small JSON object as the payload — exactly what the declared jsonschema describes,
// so the payload-fidelity tier stores bytes Foxglove decodes with no plexus codec in the path.
struct reading_codec
{
    using value_type = reading;

    plexus::wire_bytes<> encode(const reading &r) const
    {
        auto owner = std::make_shared<std::string>(
            "{\"sensor\":" + std::to_string(r.sensor) + ",\"value\":" + std::to_string(r.value) + "}");
        std::span<const std::byte> view = std::as_bytes(std::span{owner->data(), owner->size()});
        return {view, std::move(owner)};
    }

    plexus::expected<void, std::error_code> decode(std::span<const std::byte>, reading &) const
    {
        return {};
    }

    plexus::type_identity type_info() const { return {0x5E4501u, "reading"}; }
};

constexpr std::string_view k_reading_jsonschema =
    R"({"type":"object","title":"reading","properties":{)"
    R"("sensor":{"type":"integer"},"value":{"type":"integer"}}})";

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

plexus::node_options opts(std::uint64_t seed, bool eager)
{
    plexus::node_options o;
    o.redial_seed  = seed;
    o.dial_eagerly = eager;
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
    plexus::inproc::inproc_bus<> bus;
    plexus::inproc::inproc_executor<> ex{bus};
    transport_t ta{ex, bus};
    transport_t tb{ex, bus};
    plexus::discovery::static_discovery disc{{}};

    buffer_sink sink;

    {
        node_t pub_node{ex, disc, id_of(0x0A), ta, opts(0xA, /*eager=*/false)};
        node_t sub_node{ex, disc, id_of(0x0B), tb, opts(0xB, /*eager=*/true)};
        sub_node.listen({"inproc", "host-b:6000"});
        pub_node.listen({"inproc", "host-a:5000"});
        ex.drain();

        // Declare the reading type's self-description in the stream preamble: the bytes are
        // JSON and the schema is a real JSON Schema, so the transcode labels the data channel
        // {"json","jsonschema"} and Foxglove decodes it.
        plexus::recorder_options ropts;
        const auto schema_bytes =
            std::as_bytes(std::span{k_reading_jsonschema.data(), k_reading_jsonschema.size()});
        ropts.schemas.push_back(plexus::type_schema{.type_id          = 0x5E4501u,
                                                    .message_encoding = "json",
                                                    .schema_name      = "reading",
                                                    .schema_encoding  = "jsonschema",
                                                    .schema_data      = schema_bytes});
        auto rec = pub_node.make_recorder(sink, std::move(ropts));

        // Capture each topic's payload (the bare codec JSON), keeping every sample.
        plexus::typed_publisher_options pub_opts;
        pub_opts.capture = plexus::recording_qos{.fidelity = plexus::io::capture_fidelity::payload};

        {
            plexus::publisher<reading_codec> temp{
                pub_node, "telemetry.temperature", pub_opts, reading_codec{}};
            plexus::publisher<reading_codec> press{
                pub_node, "telemetry.pressure", pub_opts, reading_codec{}};
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
        while(rec.pump())
            ;
        rec.flush();
    }
    ex.drain();

    const std::filesystem::path flat_path = "mcap_basic_foxglove.plxr";
    {
        std::ofstream out{flat_path, std::ios::binary};
        out.write(reinterpret_cast<const char *>(sink.bytes().data()),
                  static_cast<std::streamsize>(sink.bytes().size()));
    }
    std::cout << "captured " << sink.bytes().size() << " bytes -> " << flat_path.string() << '\n';

    const std::filesystem::path mcap_path = "mcap_basic_foxglove.mcap";
    const auto result                     = plexus::tools::flat_to_mcap(sink.bytes(), mcap_path);
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
