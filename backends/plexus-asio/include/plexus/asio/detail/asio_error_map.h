#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_ASIO_ERROR_MAP_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_ASIO_ERROR_MAP_H

#include "plexus/io/io_error.h"

#include <asio/error.hpp>

#include <system_error>

namespace plexus::asio::detail {

// EOF folds onto broken_pipe: a clean half-close is a broken stream to the byte_channel.
inline io::io_error map_error(const std::error_code &ec)
{
    if(ec == ::asio::error::connection_refused)
        return io::io_error::connection_refused;
    if(ec == ::asio::error::connection_reset)
        return io::io_error::connection_reset;
    if(ec == ::asio::error::broken_pipe)
        return io::io_error::broken_pipe;
    if(ec == ::asio::error::eof)
        return io::io_error::broken_pipe;
    if(ec == ::asio::error::timed_out)
        return io::io_error::timed_out;
    if(ec == ::asio::error::address_in_use)
        return io::io_error::address_in_use;
    if(ec == ::asio::error::operation_aborted)
        return io::io_error::operation_aborted;
    return io::io_error::unknown;
}

}

#endif
