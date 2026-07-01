#ifndef HPP_GUARD_PLEXUS_EXAMPLE_STREAM_METER_H
#define HPP_GUARD_PLEXUS_EXAMPLE_STREAM_METER_H

#include <asio/steady_timer.hpp>

#include <chrono>
#include <cstdint>
#include <iostream>

namespace example {

// The one-way headline: delivered msgs/bytes accumulated from the "stream" topic; the periodic report
// emits the per-interval DELTA (instantaneous rate), not the cumulative total.
struct stream_meter
{
    std::uint64_t msgs{0};
    std::uint64_t bytes{0};
    std::uint64_t last_msgs{0};
    std::uint64_t last_bytes{0};
};

inline void schedule_report(::asio::steady_timer &timer, stream_meter &meter)
{
    timer.expires_after(std::chrono::seconds{1});
    timer.async_wait([&timer, &meter](const std::error_code &ec)
                     {
                         if(ec)
                             return;
                         const std::uint64_t dmsgs  = meter.msgs - meter.last_msgs;
                         const std::uint64_t dbytes = meter.bytes - meter.last_bytes;
                         meter.last_msgs  = meter.msgs;
                         meter.last_bytes = meter.bytes;
                         // std::endl (not '\n') FLUSHES each line: the runner captures stdout to a file
                         // (fully buffered, not line-buffered) and kills the peer, so an unflushed line is lost.
                         std::cout << "HOST throughput msgs=" << dmsgs << " bytes=" << dbytes << " elapsed_us=1000000 rate_msg_s=" << dmsgs << std::endl;
                         schedule_report(timer, meter);
                     });
}

}

#endif
