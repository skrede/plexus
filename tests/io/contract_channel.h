#ifndef HPP_GUARD_PLEXUS_TESTS_IO_CONTRACT_CHANNEL_H
#define HPP_GUARD_PLEXUS_TESTS_IO_CONTRACT_CHANNEL_H

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"

#include "plexus/io/detail/pending_ops.h"

#include "plexus/detail/compat.h"

#include "plexus/wire/close_cause.h"

#include <deque>
#include <span>
#include <string>
#include <vector>
#include <cstddef>
#include <utility>

namespace plexus::testing {

class fifo_executor
{
public:
    fifo_executor()
            : m_queue()
    {
    }

    void post(plexus::detail::move_only_function<void()> fn)
    {
        m_queue.push_back(std::move(fn));
    }

    bool run_one()
    {
        if(m_queue.empty())
            return false;
        auto fn = std::move(m_queue.front());
        m_queue.pop_front();
        fn();
        return true;
    }

    void drain()
    {
        while(run_one())
        {
        }
    }

    std::size_t queued() const noexcept
    {
        return m_queue.size();
    }

private:
    std::deque<plexus::detail::move_only_function<void()>> m_queue;
};

// A byte_channel honoring the close contract over a fifo_executor: close() aborts its one pending operation by
// queueing that operation's completion, and on_closed is posted once the count drains, a late install included.
class contract_channel
{
public:
    contract_channel(fifo_executor &executor, std::vector<std::string> &log)
            : m_op_pending(false)
            , m_closed_posted(false)
            , m_executor(executor)
            , m_ops()
            , m_log(log)
            , m_on_closed_cb()
            , m_on_error_cb()
            , m_on_protocol_close_cb()
            , m_on_data_cb()
    {
        m_ops.on_settled([this] { post_closed(); });
    }

    ~contract_channel()
    {
        m_log.push_back("destroyed");
    }

    void start_operation()
    {
        m_ops.begin();
        m_op_pending = true;
    }

    void fail(io::io_error error)
    {
        if(m_ops.closing())
            return;
        if(m_on_error_cb)
            m_on_error_cb(error);
        close();
    }

    void send(std::span<const std::byte>)
    {
    }

    void close()
    {
        if(m_ops.closing())
            return;
        detach();
        abort_operation();
        m_ops.close();
    }

    io::endpoint remote_endpoint() const
    {
        return {"fake", "contract"};
    }

    void on_closed(plexus::detail::move_only_function<void()> cb)
    {
        m_on_closed_cb = std::move(cb);
        post_closed();
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
    bool m_op_pending;
    bool m_closed_posted;
    fifo_executor &m_executor;
    io::detail::pending_ops m_ops;
    std::vector<std::string> &m_log;
    plexus::detail::move_only_function<void()> m_on_closed_cb;
    plexus::detail::move_only_function<void(io::io_error)> m_on_error_cb;
    plexus::detail::move_only_function<void(wire::close_cause)> m_on_protocol_close_cb;
    plexus::detail::move_only_function<void(std::span<const std::byte>)> m_on_data_cb;

    void detach()
    {
        m_on_error_cb          = nullptr;
        m_on_protocol_close_cb = nullptr;
        m_on_data_cb           = nullptr;
    }

    void abort_operation()
    {
        if(!m_op_pending)
            return;
        m_op_pending = false;
        m_executor.post(
                [this]
                {
                    m_ops.end();
                    m_log.push_back("operation");
                });
    }

    void post_closed()
    {
        if(!m_ops.settled() || !m_on_closed_cb || m_closed_posted)
            return;
        m_closed_posted = true;
        m_executor.post(
                [this]
                {
                    m_log.push_back("closed");
                    auto cb = std::exchange(m_on_closed_cb, nullptr);
                    cb();
                });
    }
};

}

#endif
