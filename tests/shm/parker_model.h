#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_PARKER_MODEL_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_PARKER_MODEL_H

#include "park_enumerator.h"

#include "support/registered_waiters.h"

#include "plexus/shm/ring_layout.h"
#include "plexus/shm/waiter_park.h"

#include <array>
#include <atomic>
#include <cstdint>
#include <utility>
#include <optional>
#include <functional>

namespace plexus::testing {

// The per-slot parker over a real ring header. A block registers only while the waiter's own word still reads PARKED
// (the FUTEX_WAIT value compare on the park word), and a wake releases exactly the slots the producer collected.
struct parker_model
{
    static constexpr std::uint32_t k_waiters = 2;

    parker_model()
            : proceed()
            , header()
            , snapshot()
            , blocked_on()
    {
        register_waiters(header, k_waiters);
    }

    std::array<bool, k_waiters> proceed;
    plexus::shm::control_header_t header;
    std::array<std::uint32_t, k_waiters> snapshot;
    std::array<std::optional<std::uint32_t>, k_waiters> blocked_on;

    void block(std::uint32_t slot)
    {
        if(proceed[slot] && plexus::shm::park_word(header, slot).load() == plexus::shm::k_park_parked)
            blocked_on[slot] = snapshot[slot];
    }

    void wake(std::uint32_t mask)
    {
        plexus::shm::wake_parked(header, mask,
                                 [this](std::atomic<std::uint32_t> &word)
                                 {
                                     for(std::uint32_t slot = 0; slot < k_waiters; ++slot)
                                     {
                                         if(&plexus::shm::park_word(header, slot) == &word)
                                             blocked_on[slot].reset();
                                     }
                                 });
    }

    bool no_lost_wake() const
    {
        const std::uint32_t final_generation = header.notify_generation.load();
        for(const std::optional<std::uint32_t> &blocked : blocked_on)
        {
            if(blocked && *blocked != final_generation)
                return false;
        }
        return true;
    }
};

using parker_sequence = interleaving_enumerator<parker_model>::sequence;

// A blocked waiter's later steps are skipped, as in model_step.
inline parker_sequence::value_type parker_step(std::uint32_t slot, std::function<void(parker_model &)> action)
{
    return [slot, action = std::move(action)](parker_model &model)
    {
        if(!model.blocked_on[slot])
            action(model);
    };
}

inline parker_sequence parked_waiter(std::uint32_t slot)
{
    return {
            parker_step(slot, [slot](parker_model &m) { m.snapshot[slot] = plexus::shm::park_snapshot(m.header); }),
            parker_step(slot, [slot](parker_model &m) { plexus::shm::park_store(m.header, slot); }),
            parker_step(slot, [slot](parker_model &m) { m.proceed[slot] = plexus::shm::park_recheck(m.header, m.snapshot[slot]); }),
            parker_step(slot, [slot](parker_model &m) { m.block(slot); }),
            parker_step(slot, [slot](parker_model &m) { plexus::shm::park_clear(m.header, slot); }),
    };
}

inline parker_sequence parker_signals(std::uint32_t count)
{
    parker_sequence steps;
    for(std::uint32_t i = 0; i < count; ++i)
    {
        steps.push_back([](parker_model &m) { plexus::shm::bump_generation(m.header); });
        steps.push_back([](parker_model &m) { m.wake(plexus::shm::collect_parked(m.header)); });
    }
    return steps;
}

}

#endif
