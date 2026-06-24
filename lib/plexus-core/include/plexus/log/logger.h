#ifndef HPP_GUARD_PLEXUS_LOG_LOGGER_H
#define HPP_GUARD_PLEXUS_LOG_LOGGER_H

#include <string_view>

namespace plexus::log {

class logger
{
public:
    virtual ~logger() = default;

    virtual void warn(std::string_view message) = 0;
};

class null_logger final : public logger
{
public:
    void warn(std::string_view) override
    {
    }
};

}

#endif
