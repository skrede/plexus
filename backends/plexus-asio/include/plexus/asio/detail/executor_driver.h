#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_EXECUTOR_DRIVER_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_EXECUTOR_DRIVER_H

#include "plexus/detail/executor_affinity.h"

#include <asio/execution_context.hpp>

#include <thread>

namespace plexus::asio::detail {

// The io_context owns this record through its service registry, so each context learns its own driver and the record
// dies with the context; the static id is asio's per-type service key.
class executor_driver : public ::asio::execution_context::service
{
public:
    inline static ::asio::execution_context::id id;

    explicit executor_driver(::asio::execution_context &context)
            : ::asio::execution_context::service(context)
            , m_affinity{}
    {
    }

    bool observe(std::thread::id self, bool inside_handler) noexcept
    {
        return m_affinity.observe(self, inside_handler);
    }

private:
    plexus::detail::executor_affinity<std::thread::id> m_affinity;

    void shutdown() override
    {
    }
};

}

#endif
