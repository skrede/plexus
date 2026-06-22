#ifndef HPP_GUARD_PLEXUS_IO_NULL_LOGGER_H
#define HPP_GUARD_PLEXUS_IO_NULL_LOGGER_H

// The io layer's logger include surface. Each consumer that needs an inert sink
// owns its own log::null_logger member and binds it by reference — there is no
// shared process-wide sink, so nothing here but the type include.
#include "plexus/log/logger.h"

#endif
