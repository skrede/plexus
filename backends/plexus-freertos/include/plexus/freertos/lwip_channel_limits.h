#ifndef HPP_GUARD_PLEXUS_FREERTOS_LWIP_CHANNEL_LIMITS_H
#define HPP_GUARD_PLEXUS_FREERTOS_LWIP_CHANNEL_LIMITS_H

#include <cstddef>

namespace plexus::freertos {

// MCU knob-down defaults: an lwip_channel-LOCAL constant set, never a mutation of the shared
// fragmentation.h ceilings (the PC build is byte-identical). The read buffer matches the lwIP
// default TCP window (5760 B = 4x the 1440 MSS) so one recv drains a full advertised window; the
// ceilings sit well under the 64 KiB / 128 KiB / 16 KiB host defaults. The RX pool's SRAM is
// rx_slots * read_buffer_bytes (~22.5 KiB here), so both knobs are kept small.
struct lwip_channel_limits
{
    static constexpr std::size_t read_buffer_bytes = 5760;        // the lwIP TCP_WND default
    static constexpr std::size_t max_message_bytes = 8 * 1024;
    static constexpr std::size_t reassembly_bytes  = 16 * 1024;
    static constexpr std::size_t egress_cap_bytes  = 8 * 1024;
    static constexpr std::size_t rx_slots          = 4;
};

}

#endif
