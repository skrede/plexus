#ifndef HPP_GUARD_TESTS_INTEGRATION_RECORDING_HOST_SINK_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_RECORDING_HOST_SINK_COMMON_H

// Shared fixture for the host recording-sink oracle: a deterministic capture (fixed clock + fixed
// sample sequence) fed to any byte_sink, plus file/stream helpers. The sink-behavior cases and the
// record-rate QoS cases compile into one target over this header.

#include "in_memory_byte_sink.h"

#include "plexus/recording/host/file_sink.h"
#include "plexus/recording/host/rotating_sink.h"

#include "plexus/io/message_info.h"
#include "plexus/io/capture_policy.h"
#include "plexus/io/observation_events.h"
#include "plexus/io/recording/flat_recorder.h"
#include "plexus/io/recording/record_format.h"
#include "plexus/io/recording/recording_sink.h"
#include "plexus/io/recording/record_rate_gate.h"
#include "plexus/io/recording/record_projection.h"
#include "plexus/io/recording/record_stream_reader.h"

#include "plexus/wire/topic_hash.h"

#include "plexus/node_id.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <string>
#include <vector>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <utility>
#include <iterator>
#include <algorithm>
#include <filesystem>

using plexus::node_id;
using plexus::io::message_info;
using plexus::io::message_view;
using plexus::io::topic_capture_rule;
using plexus::io::recording::byte_sink;
using plexus::io::recording::flat_recorder;
using plexus::io::recording::recording_sink;
using plexus::io::recording::record_category;
using plexus::io::recording::record_rate_rule;
using plexus::io::recording::read_projection_input;
using plexus::recording::host::file_sink;
using plexus::recording::host::rotating_sink;

namespace host_sink_fixture {

inline node_id make_node(std::uint8_t tag)
{
    node_id n{};
    n[0]  = std::byte{tag};
    n[15] = std::byte{0xAB};
    return n;
}

inline std::span<const std::byte> bytes_of(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

inline std::filesystem::path unique_path(const std::string &tag)
{
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::filesystem::temp_directory_path() / ("plexus_host_sink_" + tag + "_" + std::to_string(stamp));
}

inline std::vector<std::byte> read_file(const std::filesystem::path &p)
{
    std::ifstream in{p, std::ios::binary};
    const std::vector<char> raw{std::istreambuf_iterator<char>{in}, std::istreambuf_iterator<char>{}};
    std::vector<std::byte> out(raw.size());
    for(std::size_t i = 0; i < raw.size(); ++i)
        out[i] = static_cast<std::byte>(static_cast<unsigned char>(raw[i]));
    return out;
}

// One deterministic capture: a fixed clock and a fixed sample sequence, so two sinks fed this
// produce byte-identical streams.
inline void capture_session(byte_sink &sink)
{
    flat_recorder rec{sink, 64u * 1024u, [n = std::uint64_t{0}]() mutable { return ++n; }};
    rec.open(make_node(3), topic_capture_rule{});

    recording_sink tap{rec};
    for(int i = 0; i < 6; ++i)
    {
        message_info info{};
        info.publication_sequence = static_cast<std::uint64_t>(i);
        const std::string body = "sample-payload-" + std::to_string(i);
        tap.on_message_delivered("sensor/imu", info, message_view{bytes_of(body), {}});
    }
    while(rec.pump())
        ;
    rec.flush();
}

inline std::size_t count_samples(std::span<const std::byte> stream, std::uint64_t topic_hash)
{
    const auto proj = read_projection_input(stream);
    if(!proj)
        return 0;
    std::size_t n = 0;
    for(const auto &rec : proj->records)
        if(rec.category == record_category::sample && rec.topic_hash == topic_hash)
            ++n;
    return n;
}

}

#endif
