// Capture a live robotics session and convert it to an MCAP that decodes (and plots) in
// Foxglove out of the box, with the drop-in recorder ergonomics: the codec states its type id
// ONCE in type_info() and stamps a neutral robotics concept (pose) as its schema_hint; the
// capture drains through the bundled host file_sink; and the host transcode joins the hint to
// the Foxglove well-known foxglove.Pose schema. Nothing restates the type id and no schema row
// is hand-filled — the channel decoration is derived from the codec alone.
// Single process, public API only, self-terminating (no backends, no mDNS).
//
// Build (the `mcap` transcode is a host-side optional dep, not in the core / not on the MCU):
//     cmake -B build -DPLEXUS_BUILD_EXAMPLES=ON -DPLEXUS_BUILD_MCAP_TRANSCODE=ON
//     cmake --build build -j4 --target mcap_basic_foxglove && ./build/examples/mcap_basic_foxglove
// It writes mcap_basic_foxglove.{plxr,mcap} and prints the transcode summary. Open the .mcap in
// Foxglove (https://studio.foxglove.dev) or inspect it with `mcap info`/`mcap cat --json`; the
// GUI plot itself is the manual spot-check the transcode cannot self-verify.

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

#include "plexus/hints/robotics.h"

#include "plexus/recording/host/file_sink.h"

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
#include <iterator>
#include <filesystem>

using plexus::inproc::inproc_policy;
namespace robotics = plexus::hints::robotics;
using transport_t  = plexus::inproc::inproc_transport<>;
using node_t       = plexus::node<inproc_policy, transport_t>;

struct pose_sample
{
    double x{};
    double y{};
    double z{};
};

// Emits a foxglove.Pose-shaped JSON object and stamps the pose concept as its schema_hint, so
// the transcode decorates the channel with the well-known schema from the hint alone.
struct pose_codec
{
    using value_type = pose_sample;

    plexus::wire_bytes<> encode(const pose_sample &p) const
    {
        auto owner = std::make_shared<std::string>(
            "{\"position\":{\"x\":" + std::to_string(p.x) + ",\"y\":" + std::to_string(p.y) +
            ",\"z\":" + std::to_string(p.z) + "},\"orientation\":{\"x\":0,\"y\":0,\"z\":0,\"w\":1}}");
        std::span<const std::byte> view = std::as_bytes(std::span{owner->data(), owner->size()});
        return {view, std::move(owner)};
    }

    plexus::expected<void, std::error_code> decode(std::span<const std::byte>, pose_sample &) const
    {
        return {};
    }

    plexus::type_identity type_info() const
    {
        return {0x5E4501u, "robot_pose", robotics::to_hint(robotics::kind::pose)};
    }
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

std::vector<std::byte> read_flat(const std::filesystem::path &path)
{
    std::ifstream in{path, std::ios::binary};
    const std::vector<char> raw{std::istreambuf_iterator<char>{in}, std::istreambuf_iterator<char>{}};
    std::vector<std::byte> out(raw.size());
    for(std::size_t i = 0; i < raw.size(); ++i)
        out[i] = static_cast<std::byte>(static_cast<unsigned char>(raw[i]));
    return out;
}

int main()
{
    plexus::inproc::inproc_bus<> bus;
    plexus::inproc::inproc_executor<> ex{bus};
    transport_t ta{ex, bus};
    transport_t tb{ex, bus};
    plexus::discovery::static_discovery disc{{}};

    const std::filesystem::path flat_path = "mcap_basic_foxglove.plxr";

    // Drain the capture straight to disk through the bundled host file_sink — the drop-in the
    // consumer attaches instead of hand-writing a byte_sink.
    {
        plexus::recording::host::file_sink sink{flat_path};

        node_t pub_node{ex, disc, id_of(0x0A), ta, opts(0xA, /*eager=*/false)};
        node_t sub_node{ex, disc, id_of(0x0B), tb, opts(0xB, /*eager=*/true)};
        sub_node.listen({"inproc", "host-b:6000"});
        pub_node.listen({"inproc", "host-a:5000"});
        ex.drain();

        auto rec = pub_node.make_recorder(sink);

        plexus::typed_publisher_options pub_opts;
        pub_opts.capture = plexus::recording_qos{.fidelity = plexus::io::capture_fidelity::payload};

        {
            plexus::publisher<pose_codec> odom{pub_node, "robot.pose", pub_opts, pose_codec{}};
            plexus::subscriber<pose_codec> odom_sub{sub_node, "robot.pose", [](const pose_sample &) {}};
            ex.drain();

            for(std::uint32_t i = 0; i < 8; ++i)
            {
                odom.publish(pose_sample{static_cast<double>(i), 2.0 * i, 0.0});
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

    const std::vector<std::byte> flat = read_flat(flat_path);
    std::cout << "captured " << flat.size() << " bytes -> " << flat_path.string() << '\n';

    // Decorate the pose channel from the hint the codec carried: the well-known translator maps
    // the neutral concept to the Foxglove foxglove.Pose schema (an empty provider — no override).
    const std::filesystem::path mcap_path = "mcap_basic_foxglove.mcap";
    const auto result                     = plexus::tools::flat_to_mcap(flat, mcap_path, plexus::tools::schema_provider{},
                                                                        plexus::tools::well_known_schema_translator());
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
