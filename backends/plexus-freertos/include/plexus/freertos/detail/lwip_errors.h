#ifndef HPP_GUARD_PLEXUS_FREERTOS_DETAIL_LWIP_ERRORS_H
#define HPP_GUARD_PLEXUS_FREERTOS_DETAIL_LWIP_ERRORS_H

#include "plexus/io/io_error.h"

#include <system_error>

namespace plexus::freertos::detail {

inline plexus::io::io_error map_error(std::error_code ec)
{
    // Through SO_ERROR lwIP reports a RST answering the SYN as ECONNRESET, and a connect its tcp_slowtmr gave up on
    // after TCP_SYNMAXRTX retransmissions as ECONNABORTED (ERR_ABRT).
    if(ec == std::errc::connection_refused || ec == std::errc::connection_reset)
        return plexus::io::io_error::connection_refused;
    if(ec == std::errc::timed_out || ec == std::errc::connection_aborted)
        return plexus::io::io_error::timed_out;
    if(ec == std::errc::address_in_use)
        return plexus::io::io_error::address_in_use;
    return plexus::io::io_error::unknown;
}

}

#endif
