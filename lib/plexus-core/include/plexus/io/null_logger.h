#ifndef HPP_GUARD_PLEXUS_IO_NULL_LOGGER_H
#define HPP_GUARD_PLEXUS_IO_NULL_LOGGER_H

#include "plexus/log/logger.h"

namespace plexus::io {

// The shared null sink an io-layer consumer (forwarder, router) is injected with
// when the caller supplies no logger: the warn-and-drop seam exists, but is
// silent. A function-local static (no static singleton object at namespace
// scope) bound by reference, so every default consumer shares one inert sink.
inline log::logger &shared_null_logger()
{
    static log::null_logger sink;
    return sink;
}

}

#endif
