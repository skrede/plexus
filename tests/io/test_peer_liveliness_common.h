#ifndef HPP_GUARD_PLEXUS_TESTS_IO_TEST_PEER_LIVELINESS_COMMON_H
#define HPP_GUARD_PLEXUS_TESTS_IO_TEST_PEER_LIVELINESS_COMMON_H

// Shared harness for the peer-liveliness arbiter oracle: the fusion-matrix cases and the
// precedence/hysteresis cases compile into one target, so the driver, the node_id maker, the
// options builder, and the subscriber-wired log live here once.

#include "plexus/io/peer_liveliness.h"
#include "plexus/io/liveliness_peer_storage.h"

#include "plexus/node_id.h"

#include <vector>
#include <cstddef>
#include <cstdint>

using plexus::node_id;
using plexus::io::combine;
using plexus::io::fixed_liveliness_peer_storage;
using plexus::io::liveliness_options;
using plexus::io::liveliness_signal;
using plexus::io::liveliness_verdict;
using plexus::io::peer_liveliness;
using plexus::io::peer_liveliness_event;

namespace {

constexpr std::uint64_t k_interval_ns = 100'000'000;
constexpr std::uint64_t k_window_ns   = 500'000'000; // heartbeat_miss_limit (5) * interval
constexpr std::uint64_t k_base_ns     = 1'000'000'000;

node_id id_of(std::uint8_t b)
{
    node_id id{};
    id[0] = std::byte{b};
    return id;
}

liveliness_options opts_for(combine policy)
{
    liveliness_options o;
    o.policy = policy;
    return o;
}

class harness
{
public:
    explicit harness(combine policy) : arb(opts_for(policy))
    {
        arb.add_subscriber();
        arb.on_verdict([this](const peer_liveliness_event &e) { log.push_back(e); });
    }

    peer_liveliness<> arb;
    std::vector<peer_liveliness_event> log;
};

} // namespace

#endif
