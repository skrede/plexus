#ifndef HPP_GUARD_PLEXUS_IO_RECORDING_RECORDING_SINK_H
#define HPP_GUARD_PLEXUS_IO_RECORDING_RECORDING_SINK_H

#include "plexus/io/observer.h"
#include "plexus/io/message_info.h"
#include "plexus/io/security_event.h"
#include "plexus/io/observation_events.h"
#include "plexus/io/detail/drop_event.h"
#include "plexus/io/recording/flat_recorder.h"

#include "plexus/wire/topic_hash.h"

#include "plexus/detail/compat.h"

#include <span>
#include <cstddef>
#include <utility>
#include <cstdint>
#include <optional>
#include <string_view>

namespace plexus::io::recording {

// The primary semantic tap: an observer that encodes every posted edge into a recorder.
// observes_data_path() returns true, so attaching it auto-feeds the capture gate's
// observer-presence — the always-on metadata floor for every topic. Each on_* forwards the
// by-value POD (cheap carrier facts) to the recorder; the message edges forward the
// borrowed message_view bytes, which the recorder COPIES into the ring before the turn
// returns (the owner's lifetime is the callback turn only). It carries the topic's type_id
// so the offline decoder resolves the codec — NEVER a codec in the tap. It holds a
// reference to the recorder (structural single-owner; no raw pointer, no shared_from_this).
//
// Parameterized on the recorder it feeds — the continuous flat_recorder (drop-newest) or
// the pre_buffer_controller (FDR drop-oldest) — since both expose the identical record_*
// drain target with no virtual indirection on the build turn.
template<typename Recorder = flat_recorder>
class recording_sink : public io::observer
{
public:
    explicit recording_sink(Recorder &recorder) noexcept
            : m_recorder(recorder)
    {
    }

    bool observes_data_path() const override
    {
        return true;
    }

    // Wire the per-topic resolved fidelity (capture_policy.rule_for(hash).fidelity) so a
    // recorded sample is stamped with its true tier; unset falls back to payload.
    void set_fidelity_resolver(plexus::detail::move_only_function<capture_fidelity(std::uint64_t)> resolver)
    {
        m_fidelity_resolver = std::move(resolver);
    }

    // Wire the per-topic producer type_id (registry.producer_type_id(hash)) so a recorded
    // sample keys to the declared schema; unset records no type_id (a valid opaque sample).
    void set_type_id_resolver(plexus::detail::move_only_function<std::optional<std::uint64_t>(std::uint64_t)> resolver)
    {
        m_type_id_resolver = std::move(resolver);
    }

    void on_message_published(std::string_view fqn, const message_view &v) override
    {
        record_message(fqn, m_empty_info, v);
    }

    void on_message_delivered(std::string_view fqn, const message_info &info, const message_view &v) override
    {
        record_message(fqn, info, v);
    }

    void on_drop(const io::detail::drop_event &e) override
    {
        m_recorder.record_drop(e);
    }
    void on_qos_change(const qos_change_event &e) override
    {
        m_recorder.record_qos_change(e);
    }
    void on_participant(const participant_event &e) override
    {
        m_recorder.record_participant(e);
    }
    void on_endpoint(std::string_view fqn, const endpoint_event &e) override
    {
        m_recorder.record_endpoint(fqn, e);
    }
    void on_security(const security_event &e) override
    {
        m_recorder.record_security(e);
    }

    void on_wire(const wire_record &rec) override
    {
        m_recorder.record_wire(rec.dir, rec.seq, rec.peer, rec.bytes);
    }

private:
    void record_message(std::string_view fqn, const message_info &info, const message_view &v)
    {
        const std::uint64_t                hash     = wire::fqn_topic_hash(fqn);
        const std::span<const std::byte>   bytes    = v;
        const capture_fidelity             fidelity = m_fidelity_resolver ? m_fidelity_resolver(hash) : capture_fidelity::payload;
        const std::optional<std::uint64_t> id       = m_type_id_resolver ? m_type_id_resolver(hash) : std::nullopt;
        m_recorder.record_sample(hash, info, id.value_or(0), id.has_value(), fidelity, bytes);
    }

    Recorder                                                                       &m_recorder;
    const message_info                                                              m_empty_info{};
    plexus::detail::move_only_function<capture_fidelity(std::uint64_t)>             m_fidelity_resolver;
    plexus::detail::move_only_function<std::optional<std::uint64_t>(std::uint64_t)> m_type_id_resolver;
};

template<typename Recorder>
recording_sink(Recorder &) -> recording_sink<Recorder>;

}

#endif
