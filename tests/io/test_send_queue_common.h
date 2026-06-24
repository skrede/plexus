#ifndef HPP_GUARD_PLEXUS_TESTS_IO_TEST_SEND_QUEUE_COMMON_H
#define HPP_GUARD_PLEXUS_TESTS_IO_TEST_SEND_QUEUE_COMMON_H

// The send_queue block oracle: a pure sans-IO drive of the generic serial outbound
// discipline with a recording stub send-sink (capturing the owned bytes + a manual
// completion trigger), no socket and no backend link (plexus::plexus only). The
// destination type is a plain int here (the block is generic over the endpoint),
// proving the block carries no UDP shape.

#include "plexus/datagram/detail/send_queue.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <vector>
#include <cstddef>
#include <utility>

using send_queue = plexus::datagram::detail::send_queue<int>;

namespace send_queue_fixture {

// A recording sink: snapshots each presented (bytes, dest) and parks the completion
// so the test drives the serial drain by hand (proving exactly-one-outstanding).
struct recorder
{
    struct sent
    {
        std::vector<std::byte> bytes;
        int dest;
    };

    std::vector<sent> calls;
    std::vector<send_queue::completion> pending;

    send_queue::send_sink sink()
    {
        return [this](std::span<const std::byte> bytes, const int &dest, send_queue::completion done)
        {
            calls.push_back(sent{std::vector<std::byte>(bytes.begin(), bytes.end()), dest});
            pending.push_back(std::move(done));
        };
    }

    void complete_front(bool ok = true)
    {
        auto done = std::move(pending.front());
        pending.erase(pending.begin());
        done(ok);
    }
};

inline std::vector<std::byte> bytes_of(std::initializer_list<int> vals)
{
    std::vector<std::byte> out;
    for(int v : vals)
        out.push_back(static_cast<std::byte>(v));
    return out;
}

}

#endif
