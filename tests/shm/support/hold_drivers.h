#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_HOLD_DRIVERS_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_HOLD_DRIVERS_H

#include "support/co_host_node.h"
#include "support/hold_topology.h"

#include <asio/post.hpp>
#include <asio/io_context.hpp>
#include <asio/executor_work_guard.hpp>

#include <atomic>
#include <chrono>
#include <thread>
#include <optional>
#include <type_traits>

namespace plexus::testing {

// Runs one node's io_context on its own thread for as long as it lives.
class context_thread
{
public:
    explicit context_thread(co_host_node &node)
            : m_node(node)
            , m_guard(::asio::make_work_guard(node.io))
            , m_thread(
                      [&node]
                      {
                          node.io.restart();
                          node.io.run();
                      })
    {
    }

    context_thread(const context_thread &)            = delete;
    context_thread &operator=(const context_thread &) = delete;

    ~context_thread()
    {
        m_guard.reset();
        m_node.io.stop();
        m_thread.join();
    }

private:
    co_host_node &m_node;
    ::asio::executor_work_guard<::asio::io_context::executor_type> m_guard;
    std::thread m_thread;
};

// Runs fn on an io_context a context_thread drives and waits until it has run there.
template<typename Fn>
void run_on(::asio::io_context &io, Fn fn)
{
    std::atomic<bool> done{false};
    ::asio::post(io,
                 [&]
                 {
                     fn();
                     done.store(true, std::memory_order_release);
                 });
    while(!done.load(std::memory_order_acquire))
        std::this_thread::sleep_for(std::chrono::milliseconds{1});
}

template<typename Fn>
std::invoke_result_t<Fn &> read_on(::asio::io_context &io, Fn fn)
{
    std::invoke_result_t<Fn &> value{};
    run_on(io, [&] { value = fn(); });
    return value;
}

// The setup thread hands each node to its own thread once, and the node stays there to the end: its handle retires on
// that thread before the thread stops, since a node's handles belong to the thread that drives it.
class hold_drivers
{
public:
    explicit hold_drivers(hold_topology &t)
            : m_topology(t)
            , m_reader()
            , m_newcomer()
            , m_publisher()
    {
    }

    hold_drivers(const hold_drivers &)            = delete;
    hold_drivers &operator=(const hold_drivers &) = delete;

    ~hold_drivers()
    {
        retire(m_publisher, m_topology.publisher, m_topology.pub);
        retire(m_reader, m_topology.reader, m_topology.reader_sub);
        retire(m_newcomer, m_topology.newcomer, m_topology.newcomer_sub);
    }

    void drive_publisher()
    {
        m_publisher.emplace(m_topology.publisher);
    }

    void drive_reader()
    {
        m_reader.emplace(m_topology.reader);
    }

    void drive_newcomer()
    {
        m_newcomer.emplace(m_topology.newcomer);
    }

private:
    hold_topology &m_topology;
    std::optional<context_thread> m_reader;
    std::optional<context_thread> m_newcomer;
    std::optional<context_thread> m_publisher;

    template<typename Handle>
    static void retire(const std::optional<context_thread> &driver, co_host_node &node, std::optional<Handle> &handle)
    {
        if(driver)
            run_on(node.io, [&handle] { handle.reset(); });
        else
            handle.reset();
    }
};

}

#endif
