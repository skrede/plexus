#ifndef HPP_GUARD_PLEXUS_IO_IO_ERROR_H
#define HPP_GUARD_PLEXUS_IO_IO_ERROR_H

#include <cstdint>

namespace plexus::io {

enum class io_error : uint8_t
{
    connection_refused,
    broken_pipe,
    connection_reset,
    timed_out,
    address_in_use,
    operation_aborted,
    // The bytes never left the node and the channel stays open (not a network drop):
    // a refused send, either over the one-datagram size cap or against a full
    // congestion=block backpressure queue respectively.
    message_too_large,
    would_block,
    unknown
};

}

#endif
