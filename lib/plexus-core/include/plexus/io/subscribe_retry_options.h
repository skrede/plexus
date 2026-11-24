#ifndef HPP_GUARD_PLEXUS_IO_SUBSCRIBE_RETRY_OPTIONS_H
#define HPP_GUARD_PLEXUS_IO_SUBSCRIBE_RETRY_OPTIONS_H

#include <chrono>
#include <cstdint>

namespace plexus::io {

// Re-send k (from 0) waits min(max_interval, first_interval * 2^k); zero attempts disables the retry.
struct subscribe_retry_options
{
    std::uint8_t max_attempts{4};
    std::chrono::milliseconds max_interval{2000};
    std::chrono::milliseconds first_interval{250};
};

}

#endif
