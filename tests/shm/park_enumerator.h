#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_PARK_ENUMERATOR_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_PARK_ENUMERATOR_H

#include <array>
#include <atomic>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>
#include <functional>

namespace plexus::testing {

struct enumeration
{
    std::uint64_t interleavings;
    std::uint64_t violations;
};

// Runs every interleaving of the step sequences, each on a fresh value-initialized State, and counts the final states
// that fail the predicate.
template<typename State>
class interleaving_enumerator
{
public:
    using step     = std::function<void(State &)>;
    using sequence = std::vector<step>;

    explicit interleaving_enumerator(std::vector<sequence> threads)
            : m_result{0, 0}
            , m_threads(std::move(threads))
            , m_next(m_threads.size(), 0)
            , m_schedule()
    {
    }

    template<typename Predicate>
    enumeration run(Predicate holds)
    {
        m_result = {0, 0};
        extend(holds);
        return m_result;
    }

private:
    enumeration m_result;
    std::vector<sequence> m_threads;
    std::vector<std::size_t> m_next;
    std::vector<std::size_t> m_schedule;

    template<typename Predicate>
    void extend(Predicate &holds)
    {
        bool complete = true;
        for(std::size_t thread = 0; thread < m_threads.size(); ++thread)
        {
            if(m_next[thread] == m_threads[thread].size())
                continue;
            complete = false;
            m_schedule.push_back(thread);
            ++m_next[thread];
            extend(holds);
            --m_next[thread];
            m_schedule.pop_back();
        }
        if(complete)
            replay(holds);
    }

    template<typename Predicate>
    void replay(Predicate &holds)
    {
        State state{};
        std::vector<std::size_t> cursor(m_threads.size(), 0);
        for(const std::size_t thread : m_schedule)
            m_threads[thread][cursor[thread]++](state);
        ++m_result.interleavings;
        if(!holds(state))
            ++m_result.violations;
    }
};

// The kernel futex as the protocol sees it: a block registers only while the word still equals the snapshot (the
// FUTEX_WAIT value compare), a wake releases every registered waiter, and a wake with nobody registered is lost.
struct futex_model
{
    static constexpr std::size_t k_threads = 3;

    std::atomic<std::uint32_t> generation;
    std::array<std::atomic<std::uint32_t>, k_threads> park;
    std::array<std::uint32_t, k_threads> snapshot;
    std::array<std::optional<std::uint32_t>, k_threads> blocked_on;

    void block(std::size_t thread)
    {
        if(generation.load(std::memory_order_seq_cst) == snapshot[thread])
            blocked_on[thread] = snapshot[thread];
    }

    void wake_all()
    {
        blocked_on.fill(std::nullopt);
    }

    bool no_lost_wake() const
    {
        const std::uint32_t final_generation = generation.load(std::memory_order_seq_cst);
        for(const std::optional<std::uint32_t> &blocked : blocked_on)
        {
            if(blocked && *blocked != final_generation)
                return false;
        }
        return true;
    }
};

// A blocked thread's later steps are skipped, which stands for running them after every other step: the predicate
// reads only who is still blocked, so the final park value does not matter.
inline interleaving_enumerator<futex_model>::step model_step(std::size_t thread, std::function<void(futex_model &)> action)
{
    return [thread, action = std::move(action)](futex_model &model)
    {
        if(!model.blocked_on[thread])
            action(model);
    };
}

}

#endif
