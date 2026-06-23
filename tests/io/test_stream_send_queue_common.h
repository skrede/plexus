#ifndef HPP_GUARD_PLEXUS_TESTS_IO_TEST_STREAM_SEND_QUEUE_COMMON_H
#define HPP_GUARD_PLEXUS_TESTS_IO_TEST_STREAM_SEND_QUEUE_COMMON_H

// The stream_send_queue block oracle: a pure sans-IO drive of the endpoint-less serial
// outbound discipline with a recording stub send-sink (capturing the gathered bytes + a
// manual completion trigger), no socket and no backend link (plexus::plexus only). It is
// the stream sibling of send_queue: the datagram block carries a per-node Endpoint, this
// one carries none (the stream sink is one async_write over a buffer SEQUENCE with no
// destination).

#include "plexus/stream/detail/send_queue.h"

#include "plexus/wire_bytes.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <memory>
#include <vector>
#include <cstddef>
#include <utility>

using stream_send_queue = plexus::stream::detail::send_queue;

namespace stream_send_queue_fixture {

// A recording sink: snapshots the SEQUENCE of bytes views presented per drain turn (one
// inner vector per gathered node) and parks the completion so the test drives the serial
// drain by hand (proving exactly-one-write-outstanding). The pointer of the first view is
// also recorded so a test can assert the owner path passes a view, not a copy.
struct recorder
{
    std::vector<std::vector<std::vector<std::byte>>> calls;          // [turn][node][bytes]
    std::vector<const std::byte *>                   first_view_ptr; // [turn] -> &views[0][0]
    std::vector<stream_send_queue::completion>       pending;

    stream_send_queue::send_sink sink()
    {
        return [this](stream_send_queue::buffer_sequence views, stream_send_queue::completion done)
        {
            std::vector<std::vector<std::byte>> turn;
            for(const auto &v : views)
                turn.emplace_back(v.begin(), v.end());
            first_view_ptr.push_back(views.empty() ? nullptr : views.front().data());
            calls.push_back(std::move(turn));
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
