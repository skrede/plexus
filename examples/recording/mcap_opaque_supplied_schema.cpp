// Capture a session whose payload is OPAQUE to plexus — a raw byte blob the store never
// interprets — and convert it to MCAP that still decodes in Foxglove because the consumer
// SUPPLIES a truthful schema (the contrast to mcap_basic_foxglove's typed codec).
//
// HONESTY CAVEAT: "opaque" means opaque to plexus's STORE, not undecodable. The
// transcode validates NOTHING about whether the bytes match the declared schema, so the
// declaration's truthfulness is the producer's contract — never declare an encoding/schema the
// bytes do not satisfy (a mismatch is a dishonest demo, not a plexus defect).
// Single process, public API only, self-terminating (no backends, no mDNS).
//
// Build (the `mcap` transcode is a host-side optional dep, not in the core / not on the MCU):
//     cmake -B build -DPLEXUS_BUILD_EXAMPLES=ON -DPLEXUS_BUILD_MCAP_TRANSCODE=ON
//     cmake --build build -j4 --target mcap_opaque_supplied_schema
//     ./build/examples/mcap_opaque_supplied_schema
// It writes mcap_opaque_supplied_schema.{plxr,mcap} and prints the transcode summary; the data
// channel decodes because the SUPPLIED encoding/schema matches the stored bytes.

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

struct opaque_blob // a finished byte blob — plexus's store sees only opaque bytes
{
    std::vector<std::byte> bytes;
};

struct blob_codec
{
    using value_type = opaque_blob;

    plexus::wire_bytes<> encode(const opaque_blob &b) const
    {
        auto                       owner = std::make_shared<std::vector<std::byte>>(b.bytes);
        std::span<const std::byte> view{owner->data(), owner->size()};
        return {view, std::move(owner)};
    }

    plexus::expected<void, std::error_code> decode(std::span<const std::byte>, opaque_blob &) const
    {
        return {};
    }

    plexus::type_identity type_info() const { return {0x0BACED01u, "opaque_blob"}; }
};

// The exact encoding the supplied bytes satisfy (the blob built below IS valid JSON for it).
constexpr std::string_view k_blob_jsonschema =
        R"({"type":"object","title":"opaque_blob","properties":{)"
        R"("seq":{"type":"integer"},"reading":{"type":"integer"}}})";

opaque_blob make_blob(std::uint32_t seq, std::uint32_t reading)
{
    const std::string json =
            "{\"seq\":" + std::to_string(seq) + ",\"reading\":" + std::to_string(reading) + "}";
    opaque_blob b;
    b.bytes.assign(reinterpret_cast<const std::byte *>(json.data()),
                   reinterpret_cast<const std::byte *>(json.data()) + json.size());
    return b;
}

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
    plexus::inproc::inproc_bus<>        bus;
    plexus::inproc::inproc_executor<>   ex{bus};
    transport_t                         ta{ex, bus};
    transport_t                         tb{ex, bus};
    plexus::discovery::static_discovery disc{{}};

    buffer_sink sink;

    {
        node_t pub_node{ex, disc, id_of(0x0A), ta, opts(0xA, /*eager=*/false)};
        node_t sub_node{ex, disc, id_of(0x0B), tb, opts(0xB, /*eager=*/true)};
        sub_node.listen({"inproc", "host-b:6000"});
        pub_node.listen({"inproc", "host-a:5000"});
        ex.drain();

        // Supply the encoding + schema the opaque bytes satisfy (copied verbatim into the
        // preamble).
        plexus::recorder_options ropts;
        const auto               schema_bytes =
                std::as_bytes(std::span{k_blob_jsonschema.data(), k_blob_jsonschema.size()});
        ropts.schemas.push_back(plexus::type_schema{.type_id          = 0x0BACED01u,
                                                    .message_encoding = "json",
                                                    .schema_name      = "opaque_blob",
                                                    .schema_encoding  = "jsonschema",
                                                    .schema_data      = schema_bytes});
        auto rec = pub_node.make_recorder(sink, std::move(ropts));

        plexus::typed_publisher_options pub_opts;
        pub_opts.capture = plexus::recording_qos{.fidelity = plexus::io::capture_fidelity::payload};

        {
            plexus::publisher<blob_codec>  sensor{pub_node, "telemetry.sensor", pub_opts,
                                                  blob_codec{}};
            plexus::subscriber<blob_codec> sensor_sub{sub_node, "telemetry.sensor",
                                                      [](const opaque_blob &) {}};
            ex.drain();

            for(std::uint32_t i = 0; i < 8; ++i)
            {
                sensor.publish(make_blob(i, 200 + i));
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

    const std::filesystem::path flat_path = "mcap_opaque_supplied_schema.plxr";
    {
        std::ofstream out{flat_path, std::ios::binary};
        out.write(reinterpret_cast<const char *>(sink.bytes().data()),
                  static_cast<std::streamsize>(sink.bytes().size()));
    }
    std::cout << "captured " << sink.bytes().size() << " bytes -> " << flat_path.string() << '\n';

    const std::filesystem::path mcap_path = "mcap_opaque_supplied_schema.mcap";
    const auto                  result    = plexus::tools::flat_to_mcap(sink.bytes(), mcap_path);
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
