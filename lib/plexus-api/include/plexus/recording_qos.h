#ifndef HPP_GUARD_PLEXUS_RECORDING_QOS_H
#define HPP_GUARD_PLEXUS_RECORDING_QOS_H

#include "plexus/io/capture_policy.h"

#include <cstdint>

namespace plexus {

// The consumer-facing recording declaration: a plain designated-initializer aggregate that
// names what (and how often) a topic is captured. It maps 1:1 onto io::topic_capture_rule,
// keeping the time_window MECHANISM default (pinning output Hz under a bursty publisher);
// count_n is the clock-free opt-out. The numeric decimation/window_ns are the substantiated
// KEEP-ALL defaults (drop nothing until the consumer opts in; the sweep validates the knob
// decimates exactly as specified). fidelity defaults to off, so a recording_qos a consumer
// leaves alone SELECTS NOTHING — declaring no recording QoS ships zero capture.
struct recording_qos
{
    io::capture_fidelity fidelity{io::capture_fidelity::off};
    io::decimation_mode  mode{io::decimation_mode::time_window};
    std::uint32_t        decimation{1};
    std::uint64_t        window_ns{0};

    io::topic_capture_rule to_rule() const noexcept
    {
        return io::topic_capture_rule{fidelity, mode, decimation, window_ns};
    }
};

}

#endif
