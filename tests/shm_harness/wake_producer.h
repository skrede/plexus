#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_HARNESS_WAKE_PRODUCER_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_HARNESS_WAKE_PRODUCER_H

#include "wake_block.h"

#include "plexus/asio/asio_policy.h"
#include "plexus/asio/shm/shm_notifier.h"

#include "plexus/native/region_handle.h"
#include "plexus/native/futex_notifier_primitive.h"

#include "plexus/shm/loan_status.h"
#include "plexus/shm/loaned_buffer.h"
#include "plexus/shm/broadcast_ring.h"
#include "plexus/shm/slot_publisher.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"

#include "plexus/muxify.h"

#include <chrono>
#include <random>
#include <thread>
#include <vector>
#include <cstdint>
#include <cstring>
#include <utility>
#include <algorithm>

namespace shm_harness {

using wake_policy         = plexus::muxify<plexus::asio::asio_policy>;
using production_notifier = plexus::asio::shm::shm_notifier<wake_policy>;

struct attached_ring
{
    plexus::native::region_handle control;
    plexus::native::region_handle slab;
    plexus::shm::broadcast_ring ring;
};

// Publishes bursts into an attached ring and times how long the consumers take to observe each burst's final message.
// A stall is rescued with an ungated wake so one lost wake costs one trial, not the rest of the run.
class burst_producer
{
public:
    burst_producer(plexus::shm::broadcast_ring &ring, production_notifier &notifier, wake_block &block, std::uint32_t consumers)
            : m_rng(std::random_device{}())
            , m_block(block)
            , m_published(0)
            , m_consumers(consumers)
            , m_notifier(notifier)
            , m_ring(ring)
            , m_latencies()
            , m_publisher(ring, plexus::io::reliability::reliable, plexus::io::congestion::block)
    {
    }

    bool burst(std::uint32_t count)
    {
        std::uniform_int_distribution<std::uint32_t> gap_us(0, 10);
        for(std::uint32_t i = 0; i < count; ++i)
        {
            if(!publish_one())
                return false;
            spin_for(std::chrono::microseconds(gap_us(m_rng)));
        }
        return true;
    }

    bool observe()
    {
        const auto start = std::chrono::steady_clock::now();
        if(observed_by(start + std::chrono::milliseconds(500)))
        {
            m_latencies.push_back(static_cast<std::uint32_t>(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - start).count()));
            return true;
        }
        m_block.stalls.fetch_add(1, std::memory_order_acq_rel);
        plexus::native::notifier_signal(m_ring.header().notify_generation);
        return observed_by(std::chrono::steady_clock::now() + std::chrono::seconds(5));
    }

    void report(std::uint32_t trials)
    {
        std::sort(m_latencies.begin(), m_latencies.end());
        const bool any = !m_latencies.empty();
        m_block.trials.store(trials, std::memory_order_release);
        m_block.p50_us.store(any ? m_latencies[m_latencies.size() / 2] : 0, std::memory_order_release);
        m_block.p99_us.store(any ? m_latencies[m_latencies.size() * 99 / 100] : 0, std::memory_order_release);
        m_block.max_us.store(any ? m_latencies.back() : 0, std::memory_order_release);
    }

private:
    std::mt19937 m_rng;
    wake_block &m_block;
    std::uint32_t m_published;
    std::uint32_t m_consumers;
    production_notifier &m_notifier;
    plexus::shm::broadcast_ring &m_ring;
    std::vector<std::uint32_t> m_latencies;
    plexus::shm::slot_publisher m_publisher;

    bool publish_one()
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        plexus::shm::loaned_buffer buffer;
        while(m_publisher.loan(sizeof(m_published), buffer) != plexus::shm::loan_status::ok)
        {
            if(std::chrono::steady_clock::now() > deadline)
                return false;
            std::this_thread::yield();
        }
        std::memcpy(buffer.bytes().data(), &m_published, sizeof(m_published));
        buffer.set_filled(sizeof(m_published));
        m_publisher.publish(std::move(buffer));
        ++m_published;
        m_notifier.signal();
        return true;
    }

    bool observed_by(std::chrono::steady_clock::time_point deadline) const
    {
        while(!observed())
        {
            if(std::chrono::steady_clock::now() > deadline)
                return false;
            std::this_thread::yield();
        }
        return true;
    }

    bool observed() const
    {
        for(std::uint32_t index = 0; index < m_consumers; ++index)
        {
            if(m_block.consumers[index].received.load(std::memory_order_acquire) != m_published)
                return false;
        }
        return true;
    }

    static void spin_for(std::chrono::microseconds gap)
    {
        const auto until = std::chrono::steady_clock::now() + gap;
        while(std::chrono::steady_clock::now() < until)
            ;
    }
};

}

#endif
