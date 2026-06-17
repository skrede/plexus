#ifndef HPP_GUARD_PLEXUS_IO_RECORDING_RECORDING_SINK_H
#define HPP_GUARD_PLEXUS_IO_RECORDING_RECORDING_SINK_H

#include "plexus/io/observer.h"
#include "plexus/io/message_info.h"
#include "plexus/io/security_event.h"
#include "plexus/io/observation_events.h"
#include "plexus/io/detail/drop_event.h"
#include "plexus/io/recording/flat_recorder.h"

#include "plexus/wire/topic_hash.h"

#include <span>
#include <cstddef>
#include <string_view>

namespace plexus::io::recording {

// The primary semantic tap: an observer that encodes every posted edge into the flat
// recorder. observes_data_path() returns true, so attaching it auto-feeds the capture
// gate's observer-presence — the always-on metadata floor for every topic. Each on_*
// forwards the by-value POD (cheap carrier facts) to the recorder; the message edges
// forward the borrowed message_view bytes, which the recorder COPIES into the ring before
// the turn returns (the owner's lifetime is the callback turn only). It carries the
// topic's type_id so the offline decoder resolves the codec — NEVER a codec in the tap.
// It holds a reference to the recorder (structural single-owner; no raw pointer, no
// shared_from_this).
class recording_sink : public io::observer
{
public:
    explicit recording_sink(flat_recorder &recorder) noexcept : m_recorder(recorder) {}

    bool observes_data_path() const override { return true; }

    void on_message_published(std::string_view fqn, const message_view &v) override
    {
        record_message(fqn, m_empty_info, v);
    }

    void on_message_delivered(std::string_view fqn, const message_info &info, const message_view &v) override
    {
        record_message(fqn, info, v);
    }

    void on_drop(const io::detail::drop_event &e) override { m_recorder.record_drop(e); }
    void on_qos_change(const qos_change_event &e) override { m_recorder.record_qos_change(e); }
    void on_participant(const participant_event &e) override { m_recorder.record_participant(e); }
    void on_endpoint(std::string_view fqn, const endpoint_event &e) override { m_recorder.record_endpoint(fqn, e); }
    void on_security(const security_event &e) override { m_recorder.record_security(e); }

private:
    void record_message(std::string_view fqn, const message_info &info, const message_view &v)
    {
        const std::span<const std::byte> bytes = v;
        m_recorder.record_sample(wire::fqn_topic_hash(fqn), info, 0u, false,
                                 capture_fidelity::payload, bytes);
    }

    flat_recorder     &m_recorder;
    const message_info m_empty_info{};
};

}

#endif
