#ifndef HPP_GUARD_PLEXUS_RECORDING_QOS_H
#define HPP_GUARD_PLEXUS_RECORDING_QOS_H

#include "plexus/io/capture_policy.h"

#include <cstdint>

namespace plexus {

// fidelity off selects nothing, so a recording_qos left at its defaults ships zero capture.
struct recording_qos
{
    io::capture_fidelity fidelity{io::capture_fidelity::off};
    io::decimation_mode mode{io::decimation_mode::time_window};
    std::uint32_t decimation{1};
    std::uint64_t window_ns{0};

    io::topic_capture_rule to_rule() const noexcept
    {
        return io::topic_capture_rule{fidelity, mode, decimation, window_ns};
    }
};

}

#endif
