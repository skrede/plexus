#ifndef HPP_GUARD_PLEXUS_IO_IO_ERROR_H
#define HPP_GUARD_PLEXUS_IO_IO_ERROR_H

#include <cstdint>

namespace plexus::io {

// Shared error enumeration consumed by every type satisfying the byte_channel
// concept. A backend maps its native transport errors onto these values so the
// concept surface stays asio-independent.
enum class io_error : uint8_t
{
    connection_refused,
    broken_pipe,
    connection_reset,
    timed_out,
    address_in_use,
    operation_aborted,
    unknown
};

}

#endif
