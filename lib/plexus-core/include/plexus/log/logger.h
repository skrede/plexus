#ifndef HPP_GUARD_PLEXUS_LOG_LOGGER_H
#define HPP_GUARD_PLEXUS_LOG_LOGGER_H

#include <string_view>

namespace plexus::log {

// Cold-path runtime logger interface — the second of the two locked virtual
// seams (symmetric with discovery). A runtime-injected abstract base, NOT a
// Policy member. The surface is deliberately minimal: a single warn() — only
// what the forwarder's warn-and-drop receive tail needs. No levels, formatting,
// or sinks beyond this. The message passes by value-view (no raw pointers).
class logger
{
public:
    virtual ~logger() = default;

    virtual void warn(std::string_view message) = 0;
};

// The no-op default the forwarder is injected with when the caller supplies no
// logger. warn() drops its argument; the seam exists, the sink is silent.
class null_logger final : public logger
{
public:
    void warn(std::string_view) override
    {
    }
};

}

#endif
