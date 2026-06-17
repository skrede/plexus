#ifndef HPP_GUARD_PLEXUS_IO_RECORDING_PRE_BUFFER_CONTROLLER_H
#define HPP_GUARD_PLEXUS_IO_RECORDING_PRE_BUFFER_CONTROLLER_H

#include "plexus/io/message_info.h"
#include "plexus/io/capture_policy.h"
#include "plexus/io/security_event.h"
#include "plexus/io/observation_events.h"
#include "plexus/io/detail/drop_event.h"
#include "plexus/io/recording/byte_ring.h"
#include "plexus/io/recording/byte_sink.h"
#include "plexus/io/recording/record_envelope.h"
#include "plexus/io/recording/record_stream_writer.h"

#include "plexus/wire/cursor.h"
#include "plexus/wire/varint.h"

#include "plexus/detail/compat.h"

#include <span>
#include <array>
#include <cstddef>
#include <cstdint>
#include <algorithm>
#include <string_view>

namespace plexus::io::recording {

// The pre-buffer / FDR discipline on the byte_ring: the aviation-black-box posture.
// The ring runs drop-oldest continuous-overwrite, so it always holds "the last N bytes"
// of the captured edge stream and is NEVER drained until a trigger fires. A trigger
// FREEZES a snapshot by capturing the ring's two indices {tail, head} (no buffer copy,
// no allocation — the snapshot IS two index reads); the frozen window then drains to the
// byte_sink on the caller's own turns (no thread) and the ring re-arms to keep
// overwriting. The window is byte-bounded by the ring's capacity and reports its captured
// time-span = newest_ts - oldest_ts over the held records.
//
// Triggers (mechanism-not-policy: plexus fires, the consumer arms): a manual trigger();
// a consumer-registered predicate over each built record_envelope (the "freeze on anomaly"
// edge — a drop cause, a qos verdict, a deadline-miss); and a deadline-miss that rides the
// existing drop surface as a matchable edge. The predicate type is written FULLY-QUALIFIED
// as plexus::detail::move_only_function — io::detail shadows plexus::detail in this scope.
class pre_buffer_controller
{
public:
    using clock_fn   = plexus::detail::move_only_function<std::uint64_t()>;
    using freeze_when = plexus::detail::move_only_function<bool(const record_envelope &)>;

    pre_buffer_controller(byte_sink &sink, std::size_t ring_bytes, clock_fn clock,
                          std::size_t drain_batch_bytes = 64u * 1024u)
        : m_sink(sink)
        , m_ring(ring_bytes, ring_policy::drop_oldest)
        , m_clock(std::move(clock))
        , m_drain_batch(drain_batch_bytes)
    {
    }

    // Arm the anomaly predicate. Evaluated on every record-build turn; when it returns
    // true the freeze fires automatically (the same effect as a manual trigger()).
    void on_anomaly(freeze_when pred) { m_pred = std::move(pred); }

    void record_sample(std::uint64_t topic_hash, const message_info &info,
                       std::uint64_t type_id, bool type_id_present,
                       capture_fidelity fidelity, std::span<const std::byte> payload)
    {
        const std::uint64_t ts = m_clock();
        admit(m_writer.sample(ts, topic_hash, info, type_id, type_id_present, fidelity, payload), ts,
              {record_category::sample, ts, topic_hash, io::detail::drop_cause::none, 0});
    }

    void record_drop(const io::detail::drop_event &e)
    {
        const std::uint64_t ts = m_clock();
        admit(m_writer.drop(ts, e), ts, {record_category::drop, ts, e.topic_hash, e.cause, 0});
    }

    void record_qos_change(const qos_change_event &e)
    {
        const std::uint64_t ts = m_clock();
        admit(m_writer.qos_change(ts, e), ts,
              {record_category::qos_change, ts, e.topic_hash, io::detail::drop_cause::none,
               static_cast<std::uint8_t>(e.verdict)});
    }

    void record_participant(const participant_event &e)
    {
        const std::uint64_t ts = m_clock();
        admit(m_writer.participant(ts, e), ts, {record_category::participant, ts, 0, io::detail::drop_cause::none, 0});
    }

    void record_endpoint(std::string_view fqn, const endpoint_event &e)
    {
        const std::uint64_t ts = m_clock();
        admit(m_writer.endpoint(ts, fqn, e), ts,
              {record_category::endpoint, ts, e.topic_hash, io::detail::drop_cause::none, 0});
    }

    void record_security(const security_event &e)
    {
        const std::uint64_t ts = m_clock();
        admit(m_writer.security(ts, e), ts, {record_category::security, ts, 0, io::detail::drop_cause::none, 0});
    }

    // Freeze a snapshot: capture {tail, head} (two index reads — NO buffer copy, no
    // allocation) and latch the held window's time-span. After this the window is
    // drainable; the ring keeps overwriting fresh records past the frozen head.
    void trigger() noexcept
    {
        m_frozen_tail = m_ring.tail();
        m_frozen_head = m_ring.head();
        m_oldest_ts   = read_oldest_ts(m_frozen_tail, m_frozen_head);
        m_span        = m_newest_ts >= m_oldest_ts ? m_newest_ts - m_oldest_ts : 0;
        m_frozen      = true;
    }

    [[nodiscard]] bool frozen() const noexcept { return m_frozen; }

    // The captured window's time-span = newest_ts - oldest_ts over the held records (0
    // when fewer than two records were captured). Latched at the freeze and read from the
    // envelopes' capture_ts, so it stays reportable after the window has drained.
    [[nodiscard]] std::uint64_t captured_span() const noexcept { return m_span; }

    // Drain a bounded batch of the frozen window into the sink; returns true while the
    // window still holds unread records. No thread — the caller drives it on its own turn.
    // When the window is exhausted the ring re-arms (resumes drop-oldest overwrite).
    bool pump()
    {
        if(!m_frozen)
            return false;
        if(m_ring.drain_window(m_sink, m_frozen_head, m_drain_batch))
            return true;
        m_frozen = false;
        return false;
    }

    void flush() { m_sink.flush(); }

private:
    void admit(std::span<const std::byte> record, std::uint64_t ts, const record_envelope &env)
    {
        m_newest_ts = ts;
        m_ring.try_push(record);
        if(m_pred && m_pred(env))
            trigger();
    }

    // Read the capture_ts of the oldest resident record in [from, to) without allocating:
    // copy the framed record's small prefix into a stack buffer (the ring may wrap), skip
    // the [varint len][u8 category] header, and read the u64 capture_ts that every category
    // writes first. Returns the newest_ts (an empty window) when no record is resident.
    [[nodiscard]] std::uint64_t read_oldest_ts(std::uint64_t from, std::uint64_t to) const noexcept
    {
        if(from >= to)
            return m_newest_ts;
        std::array<std::byte, 24> head{};
        const std::size_t n = std::min<std::size_t>(head.size(), static_cast<std::size_t>(to - from));
        for(std::size_t i = 0; i < n; ++i)
            head[i] = m_ring.at(from + i);
        std::size_t off = 0;
        const auto  len = wire::read_varint({head.data(), n}, off);
        if(!len || off + 1u + sizeof(std::uint64_t) > n)
            return m_newest_ts;
        wire::reader r{std::span<const std::byte>{head.data() + off + 1u, sizeof(std::uint64_t)}};
        return r.u64();
    }

    byte_sink           &m_sink;
    record_stream_writer m_writer;
    byte_ring            m_ring;
    clock_fn             m_clock;
    freeze_when          m_pred;
    std::size_t          m_drain_batch;
    std::uint64_t        m_newest_ts{0};
    std::uint64_t        m_oldest_ts{0};
    std::uint64_t        m_span{0};
    std::uint64_t        m_frozen_tail{0};
    std::uint64_t        m_frozen_head{0};
    bool                 m_frozen{false};
};

}

#endif
