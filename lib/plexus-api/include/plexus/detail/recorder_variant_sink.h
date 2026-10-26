#ifndef HPP_GUARD_PLEXUS_DETAIL_RECORDER_VARIANT_SINK_H
#define HPP_GUARD_PLEXUS_DETAIL_RECORDER_VARIANT_SINK_H

#include "plexus/detail/compat.h"
#include "plexus/detail/recorder_drain.h"

#include "plexus/io/observer.h"
#include "plexus/io/message_info.h"

#include "plexus/io/recording/record_rate_gate.h"

#include "plexus/wire/topic_hash.h"

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <utility>
#include <string_view>

namespace plexus::detail {

// Dispatches every posted edge to the variant-held recorder. The two disciplines share the record_*
// contract, visited without virtual indirection.
class recorder_variant_sink : public io::observer
{
public:
    using type_id_resolver = plexus::detail::move_only_function<std::optional<std::uint64_t>(std::uint64_t)>;

    // fidelity maps a topic hash to its capture_policy-resolved tier (unset falls back to payload);
    // type_id maps it to its registered producer type_id so the sample keys to the declared schema.
    recorder_variant_sink(recorder_block &blk, plexus::detail::move_only_function<io::capture_fidelity(std::uint64_t)> fidelity, type_id_resolver type_id)
            : m_block(blk)
            , m_empty_info{}
            , m_fidelity_resolver(std::move(fidelity))
            , m_type_id_resolver(std::move(type_id))
    {
    }

    bool observes_data_path() const override
    {
        return true;
    }

    // The per-topic record-rate rules gated before the ring push (grown once here, the cold path).
    void set_record_rates(const std::vector<std::pair<std::uint64_t, io::recording::record_rate_rule>> &rules)
    {
        m_rate_gate.set_rules(rules);
    }

    // The timestamp source a max_hz rule throttles against; unset disables max_hz throttling.
    void set_clock(plexus::detail::move_only_function<std::uint64_t()> clock)
    {
        m_clock = std::move(clock);
    }

    void on_message_published(std::string_view fqn, const io::message_view &v) override
    {
        sample(fqn, m_empty_info, v);
    }

    void on_message_delivered(std::string_view fqn, const io::message_info &info, const io::message_view &v) override
    {
        sample(fqn, info, v);
    }

    void on_drop(const io::detail::drop_event &e) override
    {
        record([&](auto &m) { m.record_drop(e); });
    }

    void on_qos_change(const io::qos_change_event &e) override
    {
        record([&](auto &m) { m.record_qos_change(e); });
    }

    void on_participant(const io::participant_event &e) override
    {
        record([&](auto &m) { m.record_participant(e); });
    }

    void on_endpoint(std::string_view fqn, const io::endpoint_event &e) override
    {
        record([&](auto &m) { m.record_endpoint(fqn, e); });
    }

    void on_security(const io::security_event &e) override
    {
        record([&](auto &m) { m.record_security(e); });
    }

    void on_wire(const io::recording::wire_record &rec) override
    {
        record([&](auto &m) { m.record_wire(rec.dir, rec.seq, rec.peer, rec.bytes); });
    }

private:
    void sample(std::string_view fqn, const io::message_info &info, const io::message_view &v)
    {
        const std::uint64_t hash               = wire::fqn_topic_hash(fqn);
        if(!m_rate_gate.admit(hash, m_rate_gate.needs_clock() && m_clock ? m_clock() : 0u))
            return;
        const std::span<const std::byte> bytes = v;
        const io::capture_fidelity fidelity    = m_fidelity_resolver ? m_fidelity_resolver(hash) : io::capture_fidelity::payload;
        const std::optional<std::uint64_t> id  = m_type_id_resolver ? m_type_id_resolver(hash) : std::nullopt;
        record([&](auto &m) { m.record_sample(hash, info, id.value_or(0), id.has_value(), fidelity, bytes); });
    }

    template<typename Fn>
    void record(Fn &&fn)
    {
        std::visit(std::forward<Fn>(fn), m_block.machinery);
        m_block.kick();
    }

    recorder_block &m_block;
    const io::message_info m_empty_info;
    io::recording::record_rate_gate m_rate_gate;
    plexus::detail::move_only_function<std::uint64_t()> m_clock;
    plexus::detail::move_only_function<io::capture_fidelity(std::uint64_t)> m_fidelity_resolver;
    type_id_resolver m_type_id_resolver;
};

}

#endif
