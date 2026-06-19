#ifndef HPP_GUARD_TESTS_INTEGRATION_SELF_DESCRIBING_CAPTURE_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_SELF_DESCRIBING_CAPTURE_COMMON_H

// The self-describing capture oracle: a single-process inproc session whose producer
// declares a topic with a known producer type_id and attaches a recorder carrying a public
// type_schema, then captures, drains, and reads the flat stream back. It proves the public
// surface populates the version-2 preamble end to end: the four opaque schema fields survive
// byte-identically keyed by type_id, the per-capture crypto position equals the node's
// declared wire position, and a recovered sample carries the real producer type_id (not the
// hard-coded 0/false). A second case proves a recorder that declares nothing still writes a
// valid recoverable opaque stream, and a third proves a schema blob larger than the writer's
// default scratch round-trips (no preamble heap overrun).

#include "in_memory_byte_sink.h"

#include "plexus/node.h"
#include "plexus/expected.h"
#include "plexus/publisher.h"
#include "plexus/wire_bytes.h"
#include "plexus/recorder.h"
#include "plexus/subscriber.h"
#include "plexus/type_schema.h"
#include "plexus/typed_codec.h"
#include "plexus/node_options.h"
#include "plexus/recorder_options.h"

#include "plexus/io/capture_policy.h"
#include "plexus/io/recording/record_decode.h"
#include "plexus/io/recording/record_format.h"
#include "plexus/io/recording/record_stream_reader.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/discovery/static_discovery.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <vector>
#include <chrono>
#include <utility>
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace self_describing_fixture {

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_transport;
using plexus::discovery::static_discovery;

using bare_node = plexus::node<inproc_policy, inproc_transport<>>;

inline constexpr std::uint64_t k_reading_type_id = 0x9A9A0001u;

struct reading
{
    std::uint32_t value{};
};

struct reading_codec
{
    using value_type = reading;

    plexus::wire_bytes<> encode(const reading &v) const
    {
        auto owner = std::make_shared<std::vector<std::byte>>(4);
        for(int i = 0; i < 4; ++i)
            (*owner)[i] = static_cast<std::byte>((v.value >> (8 * i)) & 0xff);
        std::span<const std::byte> view{owner->data(), owner->size()};
        return plexus::wire_bytes<>{view, std::move(owner)};
    }

    plexus::expected<void, std::error_code> decode(std::span<const std::byte> bytes,
                                                   reading                   &out) const
    {
        if(bytes.size() != 4)
            return plexus::expected<void, std::error_code>{
                    plexus::unexpect, std::make_error_code(std::errc::invalid_argument)};
        std::uint32_t v = 0;
        for(int i = 0; i < 4; ++i)
            v |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(bytes[i])) << (8 * i);
        out.value = v;
        return {};
    }

    plexus::type_identity type_info() const { return {k_reading_type_id, "reading"}; }
};

static_assert(plexus::typed_codec<reading_codec>);

using typed_publisher  = plexus::publisher<reading_codec>;
using typed_subscriber = plexus::subscriber<reading_codec>;

inline plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

inline plexus::node_options base_opts()
{
    plexus::node_options opts;
    opts.reconnect    = plexus::io::reconnect_config{std::chrono::milliseconds(50),
                                                     std::chrono::milliseconds(2000), std::nullopt,
                                                     std::nullopt};
    opts.redial_seed  = 0xD00Du;
    opts.dial_eagerly = true;
    return opts;
}

inline std::span<const std::byte> as_bytes(std::string_view s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

// Publish one reading through a payload-fidelity typed publisher and drain the recorder out.
template<typename Recorder>
inline void capture_one(bare_node &producer, bare_node &consumer, inproc_executor<> &ex,
                        Recorder &recorder, std::uint32_t value)
{
    typed_subscriber sub{consumer, "telemetry", [](const reading &) {}};

    plexus::typed_publisher_options pub_opts;
    pub_opts.capture = plexus::recording_qos{.fidelity = plexus::io::capture_fidelity::payload};
    typed_publisher pub{producer, "telemetry", pub_opts, reading_codec{}};
    ex.drain();

    {
        auto loan = pub.borrow();
        REQUIRE(loan);
        loan->value = value;
        pub.publish(std::move(loan));
    }
    ex.drain();
    while(recorder.pump())
        ;
    recorder.flush();
}

}

#endif
