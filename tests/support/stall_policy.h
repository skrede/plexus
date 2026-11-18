#ifndef HPP_GUARD_PLEXUS_TESTS_SUPPORT_STALL_POLICY_H
#define HPP_GUARD_PLEXUS_TESTS_SUPPORT_STALL_POLICY_H

#include "plexus/policy.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"

#include "plexus/wire/close_cause.h"

#include "plexus/detail/compat.h"

#include "plexus/testing/inline_verbs.h"

#include <span>
#include <chrono>
#include <memory>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <system_error>

namespace plexus::testing {

// A controllable-backpressure substrate: a channel reports the occupancy the test holds, so at or above the low-water
// mark the egress scheduler bands instead of draining, and the synchronous post of inline_verbs runs every drain inline,
// so the send order is deterministic without a runtime.
struct stall_state
{
    std::size_t reported = 0;
    std::vector<std::vector<std::byte>> sends;
};

struct stall_executor
{
};

inline std::uint64_t next_stall_key() noexcept
{
    static std::uint64_t counter = 0;
    return ++counter;
}

class stall_hooks
{
public:
    void close()
    {
    }

    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)>)
    {
    }

    void on_closed(plexus::detail::move_only_function<void()>)
    {
    }

    void on_error(plexus::detail::move_only_function<void(io::io_error)>)
    {
    }

    void on_protocol_close(plexus::detail::move_only_function<void(wire::close_cause)>)
    {
    }
};

// Records every frame it is handed. Each construction draws a fresh scheduler key, as a real channel does, so a channel
// rebuilt at a freed address cannot alias the old one's band entry.
class stall_channel : public stall_hooks
{
public:
    explicit stall_channel(stall_state &st)
            : m_st(st)
            , m_key(next_stall_key())
    {
    }

    void send(std::span<const std::byte> d)
    {
        m_st.sends.emplace_back(d.begin(), d.end());
    }

    io::endpoint remote_endpoint() const
    {
        return {"tcp", "127.0.0.1:0"};
    }

    std::size_t backpressured() const noexcept
    {
        return m_st.reported;
    }

    std::uint64_t scheduler_key() const noexcept
    {
        return m_key;
    }

private:
    stall_state &m_st;
    std::uint64_t m_key;
};

// Reports an occupancy and records nothing. The remote scheme picks the forwarder's delivery tier.
class reported_stall_channel : public stall_hooks
{
public:
    explicit reported_stall_channel(std::size_t &reported, io::endpoint remote = {"tcp", "127.0.0.1:0"})
            : m_key(next_stall_key())
            , m_remote(std::move(remote))
            , m_reported(reported)
    {
    }

    void send(std::span<const std::byte>)
    {
    }

    io::endpoint remote_endpoint() const
    {
        return m_remote;
    }

    std::size_t backpressured() const noexcept
    {
        return m_reported;
    }

    std::uint64_t scheduler_key() const noexcept
    {
        return m_key;
    }

private:
    std::uint64_t m_key;
    io::endpoint m_remote;
    std::size_t &m_reported;
};

class stall_timer
{
public:
    explicit stall_timer(stall_executor &)
    {
    }

    stall_timer(stall_executor &, std::error_code &)
    {
    }

    void expires_after(std::chrono::milliseconds)
    {
    }

    void async_wait(plexus::detail::move_only_function<void(std::error_code)>)
    {
    }

    void cancel()
    {
    }
};

template<typename Channel>
struct basic_stall_policy : inline_verbs<stall_executor &>
{
    using byte_channel_type = Channel;
    using timer_type        = stall_timer;
    using byte_owner        = std::shared_ptr<const void>;
};

using stall_policy          = basic_stall_policy<stall_channel>;
using reported_stall_policy = basic_stall_policy<reported_stall_channel>;

static_assert(Policy<stall_policy>);
static_assert(Policy<reported_stall_policy>);

}

#endif
