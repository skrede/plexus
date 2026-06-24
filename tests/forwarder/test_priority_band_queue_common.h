#pragma once

#include "plexus/io/detail/priority_band_queue.h"
#include "plexus/io/congestion.h"
#include "plexus/io/priority.h"
#include "plexus/wire_bytes.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <memory>
#include <string>
#include <vector>
#include <cstddef>
#include <utility>

namespace priority_band_fixture {

using plexus::wire_bytes;
using plexus::io::priority;
using plexus::io::congestion;
using plexus::io::detail::band_of;
using plexus::io::detail::k_egress_bands;
using plexus::io::detail::k_band_depth;
using plexus::io::detail::priority_band_queue;

// Build a frame-owner the band slot holds (the production carrier): a span over an
// owning shared_ptr<vector>. Mirrors the forwarder's frame-once owner so the band
// tests drive the real slot representation rather than a borrowed span.
inline wire_bytes<> owned(const std::string &s)
{
    auto buf = std::make_shared<std::vector<std::byte>>(reinterpret_cast<const std::byte *>(s.data()), reinterpret_cast<const std::byte *>(s.data()) + s.size());
    std::span<const std::byte> view{*buf};
    return wire_bytes<>{view, std::shared_ptr<const void>{std::move(buf)}};
}

inline std::string body(const wire_bytes<> &w)
{
    return std::string{reinterpret_cast<const char *>(w.data()), w.size()};
}

// Fill a band to k_band_depth with bodies "frame0".."frame{N-1}".
inline void fill_band(priority_band_queue &q, std::size_t b)
{
    for(std::size_t i = 0; i < k_band_depth; ++i)
        REQUIRE(q.enqueue(b, congestion::block, owned("frame" + std::to_string(i))));
}

// Drain a queue front-to-back, capturing each body in send order.
inline std::vector<std::string> drain_bodies(priority_band_queue &q)
{
    std::vector<std::string> out;
    while(const auto *node = q.front_highest())
    {
        out.emplace_back(body(*node));
        q.pop_highest();
    }
    return out;
}

} // namespace priority_band_fixture
