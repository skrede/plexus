// Capture a live multi-endpoint session to a flat record stream, single process. The node
// declares a recording QoS (what + how often a topic is captured) and attaches a recorder
// over a consumer-owned byte_sink — here a trivial file sink the example defines over the
// public byte_sink seam (there is no built-in file sink; the drain destination is the
// consumer's choice). The recorder rides the node's executor turns (no thread). After the
// session the captured flat stream is written to a file; when the optional MCAP transcode
// is built, it is converted to an MCAP container Foxglove can open. The capture leg runs
// unconditionally; the MCAP step is the optional offline-analysis path. Public API only;
// self-terminating (no backends, no mDNS).

#include "plexus/node.h"
#include "plexus/expected.h"
#include "plexus/publisher.h"
#include "plexus/subscriber.h"
#include "plexus/wire_bytes.h"
#include "plexus/recorder.h"
#include "plexus/typed_codec.h"
#include "plexus/node_options.h"
#include "plexus/recording_qos.h"

#include "plexus/io/message_info.h"
#include "plexus/io/recording/byte_sink.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/discovery/static_discovery.h"

#ifdef PLEXUS_WITH_MCAP_TRANSCODE
#include "plexus/tools/flat_to_mcap.h"
#endif

#include <span>
#include <array>
#include <vector>
#include <chrono>
#include <memory>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <filesystem>
#include <system_error>

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

// A consumer-owned drain target over the public byte_sink seam: it accumulates the drained
// flat stream in memory and exposes it for the file write / the MCAP step. A real binding
// would write straight to a file/serial/SD here; the seam is a raw byte drain.
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
    o.reconnect = plexus::io::reconnect_config{std::chrono::milliseconds(50),
                                               std::chrono::milliseconds(2000),
                                               std::nullopt, std::nullopt};
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
    // The recording declaration: capture each selected topic's payload, keep every sample
    // (the keep-all default). Declared once at the node; a recorder gives it somewhere to
    // drain.
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

    std::cout << "captured " << sink.bytes().size() << " bytes of flat record stream\n";

    const std::filesystem::path flat_path = "recording_example_capture.plxr";
    {
        std::ofstream out{flat_path, std::ios::binary};
        out.write(reinterpret_cast<const char *>(sink.bytes().data()),
                  static_cast<std::streamsize>(sink.bytes().size()));
    }
    std::cout << "wrote the flat stream to " << flat_path.string() << '\n';

#ifdef PLEXUS_WITH_MCAP_TRANSCODE
    const std::filesystem::path mcap_path = "recording_example_capture.mcap";
    const auto result = plexus::tools::flat_to_mcap(sink.bytes(), mcap_path);
    if(result.ok)
        std::cout << "transcoded to " << mcap_path.string() << ": " << result.channels
                  << " channels, " << result.messages << " messages (open in Foxglove)\n";
    else
        std::cout << "transcode failed: " << result.error << '\n';
#else
    std::cout << "build with -DPLEXUS_BUILD_MCAP_TRANSCODE=ON to convert "
              << flat_path.string() << " to MCAP, or run: mcap_transcode "
              << flat_path.string() << " out.mcap\n";
#endif

    return 0;
}
