#ifndef HPP_GUARD_PLEXUS_IO_CAPTURE_POLICY_H
#define HPP_GUARD_PLEXUS_IO_CAPTURE_POLICY_H

#include <cstdint>
#include <unordered_map>

namespace plexus::io {

// Ordered and cumulative: a higher level subsumes every lower one (payload implies the metadata
// floor; wire implies payload).
enum class capture_fidelity : std::uint8_t
{
    off      = 0,
    metadata = 1,
    payload  = 2,
    wire     = 3,
};

// count_n keeps 1 of every N records and is clock-free; time_window keeps at most one record per
// elapsed window, pinning the output rate in Hz regardless of publish burstiness.
enum class decimation_mode : std::uint8_t
{
    count_n     = 0,
    time_window = 1,
};

// Defaults are the KEEP-ALL posture (decimation 1 keeps every record, window_ns 0 never elides).
struct topic_capture_rule
{
    capture_fidelity fidelity{capture_fidelity::off};
    decimation_mode mode{decimation_mode::time_window};
    std::uint32_t decimation{1};
    std::uint64_t window_ns{0};
};

class capture_policy
{
public:
    void set_default(topic_capture_rule rule)
    {
        m_default = rule;
    }

    topic_capture_rule default_rule() const noexcept
    {
        return m_default;
    }

    void set_topic(std::uint64_t hash, topic_capture_rule rule)
    {
        m_rules[hash] = rule;
    }

    topic_capture_rule rule_for(std::uint64_t hash) const noexcept
    {
        const auto it = m_rules.find(hash);
        return it == m_rules.end() ? m_default : it->second;
    }

    // A payload record posts iff the policy selects the topic OR a data-path observer is
    // registered.
    bool should_emit(std::uint64_t hash) const noexcept
    {
        return rule_for(hash).fidelity != capture_fidelity::off || m_observers_present > 0;
    }

    bool any_active() const noexcept
    {
        return m_observers_present > 0 || !m_rules.empty();
    }

    // Mutates the per-topic decimation state (counter or last-emit stamp).
    bool wants_payload(std::uint64_t hash, std::uint64_t now_ns) noexcept
    {
        const topic_capture_rule rule = rule_for(hash);
        if(rule.fidelity < capture_fidelity::payload)
            return false;
        if(rule.mode == decimation_mode::count_n)
        {
            const std::uint32_t n = rule.decimation == 0 ? 1 : rule.decimation;
            return (++m_counters[hash] % n) == 0;
        }
        std::uint64_t &last = m_last_emit_ns[hash];
        if(now_ns - last < rule.window_ns && last != 0)
            return false;
        last = now_ns == 0 ? 1 : now_ns;
        return true;
    }

    void add_observer() noexcept
    {
        ++m_observers_present;
    }
    void remove_observer() noexcept
    {
        if(m_observers_present > 0)
            --m_observers_present;
    }
    std::size_t observers_present() const noexcept
    {
        return m_observers_present;
    }

private:
    topic_capture_rule m_default{};
    std::unordered_map<std::uint64_t, topic_capture_rule> m_rules;
    std::unordered_map<std::uint64_t, std::uint32_t> m_counters;
    std::unordered_map<std::uint64_t, std::uint64_t> m_last_emit_ns;
    std::size_t m_observers_present{0};
};

}

#endif
