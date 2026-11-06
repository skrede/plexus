#ifndef HPP_GUARD_PLEXUS_FREERTOS_DETAIL_LWIP_ERRORS_H
#define HPP_GUARD_PLEXUS_FREERTOS_DETAIL_LWIP_ERRORS_H

#include "plexus/io/io_error.h"

#include <system_error>

namespace plexus::freertos::detail {

inline plexus::io::io_error map_error(std::error_code ec)
{
    // lwIP reports a refused or aborted connect through SO_ERROR as ECONNRESET or ECONNABORTED.
    if(ec == std::errc::connection_refused || ec == std::errc::connection_reset || ec == std::errc::connection_aborted)
        return plexus::io::io_error::connection_refused;
    if(ec == std::errc::timed_out)
        return plexus::io::io_error::timed_out;
    if(ec == std::errc::address_in_use)
        return plexus::io::io_error::address_in_use;
    return plexus::io::io_error::unknown;
}

}

#endif
