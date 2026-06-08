#ifndef HPP_GUARD_PLEXUS_TESTING_MOCK_POLICY_H
#define HPP_GUARD_PLEXUS_TESTING_MOCK_POLICY_H

#include "plexus/testing/test_clock.h"

#include "plexus/inproc/inproc_timer.h"
#include "plexus/inproc/inproc_executor.h"

#include "plexus/io/byte_channel.h"
#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/policy.h"
#include "plexus/detail/compat.h"

#include <span>
#include <vector>
#include <memory>
#include <cstddef>
#include <utility>

namespace plexus::testing {

// A pure recording byte_channel for deterministic tests. Unlike inproc_channel it
// has no bus and no partner: send() records the bytes VERBATIM into m_sent rather
// than routing them, and inbound/error/close events are driven directly through the
// channel's OWN inject_* members — never via an accessor bolted onto a production
// channel. The injection mirrors the posted-delivery contract (byte_channel): a
// handler fires only once the step-executor delivers the posted work, so a test
// observes the same never-synchronous edge a real channel guarantees. Satisfies
// plexus::io::byte_channel.
template <typename Clock = test_clock>
class mock_channel
{
public:
    explicit mock_channel(plexus::inproc::inproc_executor<Clock> &ex)
        : m_exec(&ex)
    {
    }

    mock_channel(plexus::inproc::inproc_executor<Clock> &ex, std::error_code &)
        : mock_channel(ex)
    {
    }

    mock_channel(const mock_channel &) = delete;
    mock_channel &operator=(const mock_channel &) = delete;
    mock_channel(mock_channel &&) = delete;
    mock_channel &operator=(mock_channel &&) = delete;

    void send(std::span<const std::byte> data)
    {
        m_sent.emplace_back(data.begin(), data.end());
    }

    void close() { m_closed = true; }

    [[nodiscard]] io::endpoint remote_endpoint() const { return m_remote; }

    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)> cb) { m_on_data = std::move(cb); }
    void on_closed(plexus::detail::move_only_function<void()> cb) { m_on_closed = std::move(cb); }
    void on_error(plexus::detail::move_only_function<void(io::io_error)> cb) { m_on_error = std::move(cb); }

    // Stored to satisfy the uniform channel seam and NEVER fired: a protocol-close
    // is meaningless on a non-stream channel (no partial frame is expressible).
    void on_protocol_close(plexus::detail::move_only_function<void(wire::close_cause)> cb) { m_on_protocol_close = std::move(cb); }

    // Drive an inbound frame: the on_data handler fires only after the executor
    // delivers this posted work, never synchronously from inject_inbound itself.
    void inject_inbound(std::span<const std::byte> data)
    {
        std::vector<std::byte> owned(data.begin(), data.end());
        m_exec->post([this, bytes = std::move(owned)]() mutable {
            if(m_on_data)
                m_on_data(std::span<const std::byte>(bytes));
        });
    }

    void inject_error(io::io_error err)
    {
        m_exec->post([this, err]() {
            if(m_on_error)
                m_on_error(err);
        });
    }

    void inject_close()
    {
        m_exec->post([this]() {
            if(m_on_closed)
                m_on_closed();
        });
    }

    void set_remote_endpoint(io::endpoint ep) { m_remote = std::move(ep); }

    [[nodiscard]] const std::vector<std::vector<std::byte>> &sent() const noexcept { return m_sent; }
    [[nodiscard]] std::size_t sent_packets() const noexcept { return m_sent.size(); }
    [[nodiscard]] bool closed() const noexcept { return m_closed; }

private:
    plexus::inproc::inproc_executor<Clock> *m_exec;
    io::endpoint m_remote{"mock", "peer"};
    std::vector<std::vector<std::byte>> m_sent;
    plexus::detail::move_only_function<void(std::span<const std::byte>)> m_on_data;
    plexus::detail::move_only_function<void()> m_on_closed;
    plexus::detail::move_only_function<void(io::io_error)> m_on_error;
    plexus::detail::move_only_function<void(wire::close_cause)> m_on_protocol_close;
    bool m_closed{false};
};

// The deterministic test Policy: reuses the inproc step-executor and virtual-clock
// timer (this is a DRY consolidation, not a new mechanism) and binds the recording
// mock as its byte_channel. The static_assert is the compile-time gate proving the
// substrate satisfies the seam.
struct mock_policy
{
    using executor_type = plexus::inproc::inproc_executor<test_clock> &;
    using byte_channel_type = mock_channel<test_clock>;
    using timer_type = plexus::inproc::inproc_timer<test_clock>;
    using byte_owner = std::shared_ptr<const void>;

    static void post(executor_type ex, plexus::detail::move_only_function<void()> fn)
    {
        ex.post(std::move(fn));
    }
};

}

static_assert(plexus::Policy<plexus::testing::mock_policy>,
    "mock_policy must satisfy Policy — check the mock_channel/timer constructors and post()");

#endif
