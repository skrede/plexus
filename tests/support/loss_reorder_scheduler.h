#ifndef HPP_GUARD_PLEXUS_TESTS_SUPPORT_LOSS_REORDER_SCHEDULER_H
#define HPP_GUARD_PLEXUS_TESTS_SUPPORT_LOSS_REORDER_SCHEDULER_H

#include <span>
#include <deque>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>

namespace plexus::testing {

// A deterministic LCG (Numerical Recipes constants), copied verbatim from the anti-replay window
// sweep so the loss schedule is byte-identical across runs and platforms.
class shim_lcg
{
public:
    explicit shim_lcg(std::uint64_t seed) noexcept
            : m_state(seed)
    {
    }

    std::uint64_t next() noexcept
    {
        m_state = m_state * 6364136223846793005ull + 1442695040888963407ull;
        return m_state >> 17u;
    }

    // A fixed-point fraction comparison in [0, 1): returns true with probability ~num/den.
    bool hits(std::uint32_t num, std::uint32_t den) noexcept
    {
        return den != 0 && static_cast<std::uint32_t>(next() % den) < num;
    }

    std::size_t bounded(std::size_t n) noexcept
    {
        return n == 0 ? 0 : static_cast<std::size_t>(next() % n);
    }

private:
    std::uint64_t m_state;
};

// The shim's configuration: a loss fraction as loss_num/loss_den (so a fixed-seed integer
// decision stays RNG-free), a bounded reorder depth (0 = in order), and the LCG seed. Set once
// at construction — never a mutable setter (the determinism contract: schedule fixed at ctor).
struct loss_reorder_config
{
    std::uint32_t loss_num    = 0;
    std::uint32_t loss_den    = 100;
    std::size_t reorder_depth = 0;
    std::uint64_t seed        = 0x9E3779B97F4A7C15ull;
};

// A pure (no-IO) deterministic loss/reorder scheduler over an opaque datagram stream. drive()
// consumes one datagram and returns the SET to emit now (empty if dropped or held; several when
// a held burst flushes). flush() drains any datagrams still held at end-of-stream. The emitted
// order across a fixed input + seed is identical on every run, asserted by the shim's self-test.
class loss_reorder_scheduler
{
public:
    explicit loss_reorder_scheduler(loss_reorder_config cfg)
            : m_cfg(cfg)
            , m_rng(cfg.seed)
    {
    }

    std::vector<std::vector<std::byte>> drive(std::span<const std::byte> datagram)
    {
        std::vector<std::vector<std::byte>> out;
        if(m_rng.hits(m_cfg.loss_num, m_cfg.loss_den))
        {
            ++m_dropped;
            return out;
        }
        std::vector<std::byte> dg(datagram.begin(), datagram.end());
        if(m_cfg.reorder_depth == 0)
        {
            out.push_back(std::move(dg));
            return out;
        }
        m_hold.push_back(std::move(dg));
        if(m_hold.size() > m_cfg.reorder_depth)
            out.push_back(take_held());
        return out;
    }

    std::vector<std::vector<std::byte>> flush()
    {
        std::vector<std::vector<std::byte>> out;
        while(!m_hold.empty())
            out.push_back(take_held());
        return out;
    }

    std::size_t dropped() const noexcept
    {
        return m_dropped;
    }

private:
    // Pull one held datagram at a seed-chosen position so the re-emission order is shuffled
    // within the bounded window (not strictly FIFO) yet deterministic.
    std::vector<std::byte> take_held()
    {
        const std::size_t idx     = m_rng.bounded(m_hold.size());
        std::vector<std::byte> dg = std::move(m_hold[idx]);
        m_hold.erase(m_hold.begin() + static_cast<std::ptrdiff_t>(idx));
        return dg;
    }

    loss_reorder_config m_cfg;
    shim_lcg m_rng;
    std::deque<std::vector<std::byte>> m_hold;
    std::size_t m_dropped = 0;
};

}

#endif
