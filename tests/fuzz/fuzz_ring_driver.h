#ifndef HPP_GUARD_PLEXUS_TESTS_FUZZ_FUZZ_RING_DRIVER_H
#define HPP_GUARD_PLEXUS_TESTS_FUZZ_FUZZ_RING_DRIVER_H

#include "fuzz_sink.h"
#include "fuzz_region.h"

#include "support/noop_notifier.h"

#include "plexus/shm/detail/shm_ring_lifetime.h"

#include "plexus/shm/loan_status.h"
#include "plexus/shm/shm_channel.h"
#include "plexus/shm/broadcast_ring.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"

#include <atomic>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <numeric>
#include <algorithm>

namespace fuzz_ring {

using plexus::shm::loan_status;
using plexus::shm::shm_channel;
using plexus::shm::broadcast_ring;
using lifetime = plexus::shm::detail::ring_lifetime;

inline constexpr std::uint32_t k_max_steps = 64;

struct reader
{
    std::uint32_t index;
    std::uint64_t position;
};

class ring_driver
{
public:
    ring_driver(broadcast_ring &ring, fuzz_region &control, fuzz_input &in) noexcept
            : m_in(in)
            , m_ring(ring)
            , m_control(control)
            , m_readers()
            , m_notifier()
            , m_channel(ring, m_notifier, plexus::io::reliability::reliable, plexus::io::congestion::block)
    {
    }

    void run()
    {
        const std::uint32_t steps = m_in.u8() % (k_max_steps + 1);
        for(std::uint32_t i = 0; i < steps && !m_in.exhausted(); ++i)
            step(m_in.u8());
    }

private:
    fuzz_input &m_in;
    broadcast_ring &m_ring;
    fuzz_region &m_control;
    std::vector<reader> m_readers;
    plexus::testing::noop_notifier m_notifier;
    shm_channel<plexus::testing::noop_notifier> m_channel;

    void step(std::uint8_t op)
    {
        switch(op % 9)
        {
            case 0:
                return register_reader();
            case 1:
                return produce(m_in.u8());
            case 2:
                return consume();
            case 3:
                return unregister_reader();
            case 4:
                return fuzz_consume(m_channel.place_reader(static_cast<std::uint32_t>(m_in.any())));
            case 5:
                return m_control.tamper(m_in, 1);
            case 6:
                return reclaim(m_in.u8());
            case 7:
                return drain();
            default:
                lifetime::repair(m_ring);
                return fuzz_consume(lifetime::retired(m_ring));
        }
    }

    void drain()
    {
        fuzz_consume(m_channel.engage_subscriber());
        shm_channel<plexus::testing::noop_notifier>::deliver_fn deliver = [](auto bytes) { fuzz_consume(bytes.size()); };
        m_channel.drain(deliver);
    }

    void register_reader()
    {
        std::uint32_t index = 0;
        if(m_ring.register_cursor(index) != loan_status::ok)
            return;
        m_readers.push_back({index, m_ring.tail_position()});
        m_ring.publish_cursor(index, m_readers.back().position);
    }

    void unregister_reader()
    {
        if(m_readers.empty())
            return;
        const auto at = m_readers.begin() + static_cast<std::ptrdiff_t>(m_in.u8() % m_readers.size());
        m_ring.unregister_cursor(at->index);
        m_readers.erase(at);
    }

    void reclaim(std::uint8_t how)
    {
        auto alive = [how](std::uint32_t) { return (how & 1) != 0; };
        fuzz_consume(m_ring.reclaim_cursor(static_cast<std::uint32_t>(m_in.any()), alive));
    }

    void produce(std::uint8_t how)
    {
        const auto rel  = (how & 1) != 0 ? plexus::io::reliability::reliable : plexus::io::reliability::best_effort;
        const auto size = static_cast<std::size_t>(m_in.any());
        broadcast_ring::claim_result claimed;
        const loan_status st = m_ring.claim_with_policy(size, rel, plexus::io::congestion::block, claimed);
        fuzz_consume(st);
        if(st != loan_status::ok)
            return;
        std::memset(claimed.slab.data(), how, std::min(size, claimed.slab.size()));
        if((how & 2) != 0)
            return m_ring.abandon(claimed.position);
        fuzz_consume(m_ring.commit(claimed.position, (how & 4) != 0 ? size : static_cast<std::size_t>(m_in.any())));
    }

    void consume()
    {
        if(m_readers.empty())
            return;
        reader &r = m_readers[m_in.u8() % m_readers.size()];
        broadcast_ring::consume_result got;
        const loan_status st = m_ring.consume(r.position, got);
        if(st == loan_status::ok && m_ring.pin_if_current(r.position))
        {
            fuzz_consume(std::accumulate(got.slab.begin(), got.slab.end(), std::byte{0}, [](std::byte a, std::byte b) { return a ^ b; }));
            m_ring.refcount_at(r.position).fetch_sub(1, std::memory_order_seq_cst);
        }
        r.position = st == loan_status::lagged ? got.position : st == loan_status::empty ? r.position : r.position + 1;
        m_ring.publish_cursor(r.index, r.position);
    }
};

}

#endif
