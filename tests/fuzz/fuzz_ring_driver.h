#ifndef HPP_GUARD_PLEXUS_TESTS_FUZZ_FUZZ_RING_DRIVER_H
#define HPP_GUARD_PLEXUS_TESTS_FUZZ_FUZZ_RING_DRIVER_H

#include "fuzz_sink.h"
#include "fuzz_region.h"
#include "fuzz_ring_readers.h"

#include "support/noop_notifier.h"

#include "plexus/shm/detail/shm_ring_lifetime.h"

#include "plexus/shm/loan_status.h"
#include "plexus/shm/shm_channel.h"
#include "plexus/shm/broadcast_ring.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <algorithm>

namespace fuzz_ring {

using lifetime = plexus::shm::detail::ring_lifetime;

inline constexpr std::uint32_t k_max_steps = 64;

class ring_driver
{
public:
    ring_driver(broadcast_ring &ring, fuzz_region &control, fuzz_input &in) noexcept
            : m_in(in)
            , m_ring(ring)
            , m_control(control)
            , m_notifier()
            , m_channel(ring, m_notifier, plexus::io::reliability::reliable, plexus::io::congestion::block)
            , m_readers(in, ring, m_channel)
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
    plexus::testing::noop_notifier m_notifier;
    shm_channel<plexus::testing::noop_notifier> m_channel;
    ring_readers m_readers;

    void step(std::uint8_t op)
    {
        switch(op % 9)
        {
            case 0:
                return m_readers.register_reader();
            case 1:
                return produce(m_in.u8());
            case 2:
                return m_readers.consume();
            case 3:
                return m_readers.unregister_reader();
            case 4:
                return m_readers.place();
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
};

}

#endif
