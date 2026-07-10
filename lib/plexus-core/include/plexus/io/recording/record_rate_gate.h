#ifndef HPP_GUARD_PLEXUS_IO_RECORDING_RECORD_RATE_GATE_H
#define HPP_GUARD_PLEXUS_IO_RECORDING_RECORD_RATE_GATE_H

#include <vector>
#include <cstdint>
#include <utility>
#include <optional>
#include <unordered_map>

namespace plexus::io::recording {

// A per-topic recording-rate rule. max_hz throttles to at most H admitted samples per second;
// every_nth keeps 1 of every N arrivals; enabled=false records nothing for the topic. An absent
// field imposes no constraint; a topic with no rule always records.
struct record_rate_rule
{
    std::optional<double> max_hz{};
    std::optional<std::uint32_t> every_nth{};
    bool enabled{true};
};

// Decides whether a sample crosses into the recorder before the ring push. The rules and the
// per-topic counter/last-stamp state are grown once at set_rules (the cold declare path), so
// admit() only reads existing entries: no allocation on the steady-state sample path.
class record_rate_gate
{
public:
    record_rate_gate()
            : m_rules()
            , m_state()
            , m_needs_clock(false)
    {
    }

    void set_rules(const std::vector<std::pair<std::uint64_t, record_rate_rule>> &rules)
    {
        m_rules.clear();
        m_state.clear();
        m_needs_clock = false;
        for(const auto &[hash, rule] : rules)
        {
            m_rules[hash] = rule;
            m_state[hash] = topic_state{};
            m_needs_clock = m_needs_clock || rule.max_hz.has_value();
        }
    }

    // True iff some rule carries max_hz, so a caller reads its clock only when a rule needs it.
    bool needs_clock() const noexcept
    {
        return m_needs_clock;
    }

    bool admit(std::uint64_t topic_hash, std::uint64_t now_ns)
    {
        const auto rule_it = m_rules.find(topic_hash);
        if(rule_it == m_rules.end())
            return true;
        const record_rate_rule &rule = rule_it->second;
        if(!rule.enabled)
            return false;
        topic_state &st = m_state.find(topic_hash)->second;
        if(rule.every_nth && !nth_elapsed(st, *rule.every_nth))
            return false;
        if(rule.max_hz && !hz_elapsed(st, *rule.max_hz, now_ns))
            return false;
        return true;
    }

private:
    struct topic_state
    {
        std::uint64_t seen{0};
        std::uint64_t last_ns{0};
        bool has_last{false};
    };

    static bool nth_elapsed(topic_state &st, std::uint32_t every_nth)
    {
        const bool take = every_nth == 0 || (st.seen % every_nth) == 0;
        ++st.seen;
        return take;
    }

    static bool hz_elapsed(topic_state &st, double max_hz, std::uint64_t now_ns)
    {
        if(max_hz <= 0.0)
            return false;
        const std::uint64_t min_gap = static_cast<std::uint64_t>(1.0e9 / max_hz);
        if(st.has_last && now_ns - st.last_ns < min_gap)
            return false;
        st.last_ns  = now_ns;
        st.has_last = true;
        return true;
    }

    std::unordered_map<std::uint64_t, record_rate_rule> m_rules;
    std::unordered_map<std::uint64_t, topic_state> m_state;
    bool m_needs_clock;
};

}

#endif
