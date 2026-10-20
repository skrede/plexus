#ifndef HPP_GUARD_PLEXUS_TESTING_MOCK_POLICY_H
#define HPP_GUARD_PLEXUS_TESTING_MOCK_POLICY_H

#include "plexus/testing/test_clock.h"

#include "plexus/inproc/inproc_timer.h"
#include "plexus/inproc/inproc_executor.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/byte_channel.h"
#include "plexus/io/detail/pending_ops.h"
#include "plexus/io/detail/close_notification.h"
#include "plexus/policy.h"
#include "plexus/detail/compat.h"

#include <span>
#include <vector>
#include <memory>
#include <cstddef>
#include <utility>

namespace plexus::testing {

// A pure recording byte_channel for deterministic tests. Unlike inproc_channel it has no bus and no partner: send()
// records the bytes verbatim into m_sent, and inbound, error and close events are driven through the channel's own
// inject_* members, never via an accessor bolted onto a production channel. Each injection is posted and counted, so
// a test observes the same never-synchronous edges, and the same close contract, a real channel guarantees.
template<typename Clock = test_clock>
class mock_channel
{
public:
    explicit mock_channel(plexus::inproc::inproc_executor<Clock> &ex)
            : m_remote{"mock", "peer"}
            , m_exec(ex)
            , m_ops()
            , m_sent()
            , m_close_notification()
    {
        m_ops.on_settled([this] { notify_closed(); });
    }

    mock_channel(plexus::inproc::inproc_executor<Clock> &ex, std::error_code &)
            : mock_channel(ex)
    {
    }

    mock_channel(const mock_channel &)            = delete;
    mock_channel &operator=(const mock_channel &) = delete;
    mock_channel(mock_channel &&)                 = delete;
    mock_channel &operator=(mock_channel &&)      = delete;

    void send(std::span<const std::byte> data)
    {
        m_sent.emplace_back(data.begin(), data.end());
    }

    void close()
    {
        if(!m_ops.closing())
            enter_closed();
    }

    [[nodiscard]] io::endpoint remote_endpoint() const
    {
        return m_remote;
    }

    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)> cb)
    {
        m_on_data_cb = std::move(cb);
    }
    void on_closed(plexus::detail::move_only_function<void()> cb)
    {
        m_close_notification.install(std::move(cb));
        notify_closed();
    }
    void on_error(plexus::detail::move_only_function<void(io::io_error)> cb)
    {
        m_on_error_cb = std::move(cb);
    }

    // Stored for the uniform channel seam and never fired: no partial frame is expressible on a non-stream channel.
    void on_protocol_close(plexus::detail::move_only_function<void(wire::close_cause)> cb)
    {
        m_on_protocol_close_cb = std::move(cb);
    }

    void inject_inbound(std::span<const std::byte> data)
    {
        inject(
                [this, bytes = std::vector<std::byte>(data.begin(), data.end())]
                {
                    if(m_on_data_cb)
                        m_on_data_cb(std::span<const std::byte>(bytes));
                });
    }

    // An injected error is the channel's fail: the handler fires, then the channel is closed.
    void inject_error(io::io_error err)
    {
        inject([this, err] { fail(err); });
    }

    void inject_close()
    {
        inject([this] { close(); });
    }

    void set_remote_endpoint(io::endpoint ep)
    {
        m_remote = std::move(ep);
    }

    [[nodiscard]] const std::vector<std::vector<std::byte>> &sent() const noexcept
    {
        return m_sent;
    }
    [[nodiscard]] std::size_t sent_packets() const noexcept
    {
        return m_sent.size();
    }
    [[nodiscard]] bool closed() const noexcept
    {
        return m_ops.closing();
    }

private:
    io::endpoint m_remote;
    plexus::inproc::inproc_executor<Clock> &m_exec;
    io::detail::pending_ops m_ops;
    std::vector<std::vector<std::byte>> m_sent;
    io::detail::close_notification m_close_notification;
    plexus::detail::move_only_function<void(io::io_error)> m_on_error_cb;
    plexus::detail::move_only_function<void(wire::close_cause)> m_on_protocol_close_cb;
    plexus::detail::move_only_function<void(std::span<const std::byte>)> m_on_data_cb;

    // A closed channel receives nothing, so it issues no new counted work.
    template<typename Event>
    void inject(Event event)
    {
        if(m_ops.closing())
            return;
        m_ops.begin();
        m_exec.post(
                [this, event = std::move(event)]() mutable
                {
                    m_ops.end();
                    event();
                });
    }

    void fail(io::io_error err)
    {
        if(m_ops.closing())
            return;
        auto on_error = std::exchange(m_on_error_cb, nullptr);
        enter_closed();
        if(on_error)
            on_error(err);
    }

    void enter_closed()
    {
        m_on_error_cb          = nullptr;
        m_on_protocol_close_cb = nullptr;
        m_on_data_cb           = nullptr;
        m_ops.close();
    }

    void notify_closed()
    {
        if(m_ops.settled())
            m_close_notification.notify([this](auto notify) { m_exec.post(std::move(notify)); });
    }
};

// The deterministic test Policy: reuses the inproc step-executor and virtual-clock
// timer (this is a DRY consolidation, not a new mechanism) and binds the recording
// mock as its byte_channel. The static_assert is the compile-time gate proving the
// substrate satisfies the seam.
struct mock_policy
{
    using executor_type     = plexus::inproc::inproc_executor<test_clock> &;
    using byte_channel_type = mock_channel<test_clock>;
    using timer_type        = plexus::inproc::inproc_timer<test_clock>;
    using byte_owner        = std::shared_ptr<const void>;

    static void post(executor_type ex, plexus::detail::move_only_function<void()> fn)
    {
        ex.post(std::move(fn));
    }
};

}

static_assert(plexus::Policy<plexus::testing::mock_policy>, "mock_policy must satisfy Policy — check the mock_channel/timer constructors and post()");

#endif
