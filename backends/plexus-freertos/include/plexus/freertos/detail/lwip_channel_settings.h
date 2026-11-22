#ifndef HPP_GUARD_PLEXUS_FREERTOS_DETAIL_LWIP_CHANNEL_SETTINGS_H
#define HPP_GUARD_PLEXUS_FREERTOS_DETAIL_LWIP_CHANNEL_SETTINGS_H

#include "plexus/io/congestion.h"

#include <cstddef>

namespace plexus::freertos::detail {

struct lwip_channel_settings
{
    std::size_t read_buffer;
    std::size_t max_message;
    std::size_t reassembly_budget;
    std::size_t egress_cap;
    plexus::io::congestion congestion;
};

}

#endif
