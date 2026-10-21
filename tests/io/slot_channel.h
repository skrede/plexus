#ifndef HPP_GUARD_PLEXUS_TESTS_IO_SLOT_CHANNEL_H
#define HPP_GUARD_PLEXUS_TESTS_IO_SLOT_CHANNEL_H

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"

#include "plexus/io/detail/pending_ops.h"
#include "plexus/io/detail/close_notification.h"

#include "plexus/detail/compat.h"

#include "plexus/wire/close_cause.h"

#include "support/race_timer.h"

#include <span>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>

namespace plexus::testing {

struct channel_probe
{
    std::int32_t sends = 0;
    bool closed        = false;
    std::vector<std::string> events;
};

// A byte_channel honoring the close contract over the race executor. hold() and release() model an operation the
// channel still has outstanding when it is retired; the probe outlives the channel, so a test reads it after erase.
class slot_channel
{
public:
    slot_channel(race_executor &executor, channel_probe &probe)
            : m_ops()
            , m_probe(probe)
            , m_executor(executor)
            , m_notification()
            , m_on_error_cb()
            , m_on_protocol_close_cb()
            , m_on_data_cb()
    {
        m_ops.on_settled([this] { notify_closed(); });
    }

    ~slot_channel()
    {
        m_probe.events.push_back("destroyed");
    }

    void hold()
    {
        m_ops.begin();
    }

    void release()
    {
        m_executor.post([this] { m_ops.end(); });
    }

    void send(std::span<const std::byte>)
    {
        ++m_probe.sends;
    }

    void close()
    {
        if(m_ops.closing())
            return;
        m_probe.closed         = true;
        m_on_error_cb          = nullptr;
        m_on_protocol_close_cb = nullptr;
        m_on_data_cb           = nullptr;
        m_ops.close();
    }

    io::endpoint remote_endpoint() const
    {
        return {"tcp", "198.51.100.7:4100"};
    }

    void on_closed(plexus::detail::move_only_function<void()> cb)
    {
        m_notification.install(std::move(cb));
        notify_closed();
    }

    void on_error(plexus::detail::move_only_function<void(io::io_error)> cb)
    {
        m_on_error_cb = std::move(cb);
    }

    void on_protocol_close(plexus::detail::move_only_function<void(wire::close_cause)> cb)
    {
        m_on_protocol_close_cb = std::move(cb);
    }

    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)> cb)
    {
        m_on_data_cb = std::move(cb);
    }

private:
    io::detail::pending_ops m_ops;
    channel_probe &m_probe;
    race_executor &m_executor;
    io::detail::close_notification m_notification;
    plexus::detail::move_only_function<void(io::io_error)> m_on_error_cb;
    plexus::detail::move_only_function<void(wire::close_cause)> m_on_protocol_close_cb;
    plexus::detail::move_only_function<void(std::span<const std::byte>)> m_on_data_cb;

    void notify_closed()
    {
        if(!m_ops.settled())
            return;
        m_notification.notify(
                [this](plexus::detail::move_only_function<void()> fn)
                {
                    m_executor.post(
                            [&probe = m_probe, fn = std::move(fn)]() mutable
                            {
                                probe.events.push_back("notified");
                                fn();
                            });
                });
    }
};

}

#endif
