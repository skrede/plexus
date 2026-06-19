#ifndef HPP_GUARD_PLEXUS_IO_CAPTURE_POLICY_H
#define HPP_GUARD_PLEXUS_IO_CAPTURE_POLICY_H

#include <cstdint>
#include <unordered_map>

namespace plexus::io {

// The recording fidelity of a topic, ordered and cumulative: a higher level subsumes
// every lower one (payload implies the metadata floor; wire implies payload). off means
// the topic is not selected for capture by the policy.
enum class capture_fidelity : std::uint8_t
{
    off      = 0,
    metadata = 1,
    payload  = 2,
    wire     = 3,
};

// How a topic's payload records are rate-limited. count_n keeps 1 of every N records and
// is clock-free (the right default where no clock is cheap, e.g. an MCU). time_window
// keeps at most one record per elapsed window, pinning the output rate in Hz regardless
// of publish burstiness.
enum class decimation_mode : std::uint8_t
{
    count_n     = 0,
    time_window = 1,
};

// A per-topic capture declaration. The default mode is time_window because pinning output
// Hz is the right host behavior under a bursty publisher (count_n leaks a burst's full
// spike through); count_n is the consumer's explicit clock-free opt-out. The numeric
// defaults are the KEEP-ALL posture (decimation 1 keeps every record, window_ns 0 never
// elides): the recorded sweep substantiates that the gate decimates exactly 1/N (count_n)
// and pins output Hz precisely (time_window) once a consumer sets it, so keeping nothing
// dropped by default is the validated consumer-sovereign choice, not an untuned guess.
struct topic_capture_rule
{
    capture_fidelity fidelity{capture_fidelity::off};
    decimation_mode  mode{decimation_mode::time_window};
    std::uint32_t    decimation{1};
    std::uint64_t    window_ns{0};
};

// The single node-level capture-decision point. It owns BOTH the per-topic selection rules
// AND the count of registered data-path observers, so should_emit decides a payload sink
// head with one cheap branch and no back-reference into the routing engine. Cold-path
// settable (declarations and observer (de)registration are operator-driven, never on the
// per-message path); the lookup/decision path is allocation-free.
class capture_policy
{
public:
    void set_default(topic_capture_rule rule) { m_default = rule; }

    topic_capture_rule default_rule() const noexcept { return m_default; }

    void set_topic(std::uint64_t hash, topic_capture_rule rule) { m_rules[hash] = rule; }

    topic_capture_rule rule_for(std::uint64_t hash) const noexcept
    {
        const auto it = m_rules.find(hash);
        return it == m_rules.end() ? m_default : it->second;
    }

    // The single payload-sink-head gate, folding both inputs: a payload record posts iff
    // the policy selects the topic OR a data-path observer is registered. The metadata
    // floor always posts when admitted here; payload-fidelity gating (and decimation) is
    // resolved upstream on the encode path via wants_payload.
    bool should_emit(std::uint64_t hash) const noexcept
    {
        return rule_for(hash).fidelity != capture_fidelity::off || m_observers_present > 0;
    }

    // The inert-when-unused short-circuit: false iff no topic selects payload-or-above and
    // no data-path observer is registered.
    bool any_active() const noexcept { return m_observers_present > 0 || !m_rules.empty(); }

    // Does this topic want a payload encode for THIS record? True iff fidelity reaches
    // payload AND the decimation test admits the record this tick. Mutates the per-topic
    // decimation state (counter or last-emit stamp), so it is non-const.
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

    void add_observer() noexcept { ++m_observers_present; }
    void remove_observer() noexcept
    {
        if(m_observers_present > 0)
            --m_observers_present;
    }
    std::size_t observers_present() const noexcept { return m_observers_present; }

private:
    topic_capture_rule                                    m_default{};
    std::unordered_map<std::uint64_t, topic_capture_rule> m_rules;
    std::unordered_map<std::uint64_t, std::uint32_t>      m_counters;
    std::unordered_map<std::uint64_t, std::uint64_t>      m_last_emit_ns;
    std::size_t                                           m_observers_present{0};
};

}

#endif
