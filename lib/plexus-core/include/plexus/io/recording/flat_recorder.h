#ifndef HPP_GUARD_PLEXUS_IO_RECORDING_FLAT_RECORDER_H
#define HPP_GUARD_PLEXUS_IO_RECORDING_FLAT_RECORDER_H

#include "plexus/io/message_info.h"
#include "plexus/io/capture_policy.h"
#include "plexus/io/security_event.h"
#include "plexus/io/observation_events.h"
#include "plexus/io/detail/drop_event.h"
#include "plexus/io/recording/byte_ring.h"
#include "plexus/io/recording/byte_sink.h"
#include "plexus/io/recording/record_format.h"
#include "plexus/io/recording/dropout_record.h"
#include "plexus/io/recording/record_stream_writer.h"

#include "plexus/detail/compat.h"
#include "plexus/node_id.h"

#include <span>
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace plexus::io::recording {

// The reference recorder: it owns the canonical flat encoding (record_stream_writer),
// the bounded grown-once byte_ring the encoded records land in, and a reference to the
// byte_sink the cooperative drain ships them to. The recording_sink calls the typed
// record_* entry points on the observer turn; each encodes one envelope, pushes the
// framed bytes into the ring (a periodic sync_marker first), and accounts ring overflow
// in an exact dropout_run that surfaces as its own record on the next successful push.
// pump() is the cooperative drain — it pops a bounded batch into the sink and reports
// whether work remains; the self-re-posting drain caller and the public attach land
// separately. No thread is spawned here; the caller drives the drain on its own turn.
class flat_recorder
{
public:
    using clock_fn = plexus::detail::move_only_function<std::uint64_t()>;

    flat_recorder(byte_sink &sink, std::size_t ring_bytes, clock_fn clock,
                  std::size_t drain_batch_bytes = 64u * 1024u)
        : m_sink(sink)
        , m_ring(ring_bytes)
        , m_clock(std::move(clock))
        , m_drain_batch(drain_batch_bytes)
    {
    }

    // Emit the stream header + Definitions preamble straight to the sink (the head is
    // never subject to ring overflow). Call once before any record_* call.
    void open(const node_id &node, topic_capture_rule rule,
              std::span<const type_schema_entry> schema = {})
    {
        const auto head = m_writer.begin_stream(m_clock(), node, rule, schema);
        m_sink.write(head);
    }

    void record_sample(std::uint64_t topic_hash, const message_info &info,
                       std::uint64_t type_id, bool type_id_present,
                       capture_fidelity fidelity, std::span<const std::byte> payload)
    {
        admit(m_writer.sample(m_clock(), topic_hash, info, type_id, type_id_present, fidelity, payload),
              payload.size(), fidelity);
    }

    void record_drop(const io::detail::drop_event &e) { admit(m_writer.drop(m_clock(), e)); }
    void record_qos_change(const qos_change_event &e) { admit(m_writer.qos_change(m_clock(), e)); }
    void record_participant(const participant_event &e) { admit(m_writer.participant(m_clock(), e)); }
    void record_endpoint(std::string_view fqn, const endpoint_event &e) { admit(m_writer.endpoint(m_clock(), fqn, e)); }
    void record_security(const security_event &e) { admit(m_writer.security(m_clock(), e)); }

    // Pop a bounded batch of framed records from the ring into the sink; returns true
    // while the ring still holds unread records (the cooperative yield). No thread.
    bool pump() { return m_ring.drain(m_sink, m_drain_batch); }

    void flush() { m_sink.flush(); }

private:
    void admit(std::span<const std::byte> record, std::uint64_t would_be_bytes,
               capture_fidelity fidelity)
    {
        maybe_sync();
        surface_dropout();
        if(!m_ring.try_push(record))
            m_dropouts.shed(would_be_bytes, fidelity);
    }

    void admit(std::span<const std::byte> record) { admit(record, record.size(), capture_fidelity::metadata); }

    // Emit a sync marker every k_sync_interval records so a recovery scan can resync the
    // record boundary after a corrupt span. A dropped marker is harmless (the next one
    // serves), so it is not accounted as a dropout.
    void maybe_sync()
    {
        if((m_records++ % k_sync_interval) == 0)
            m_ring.try_push(m_writer.sync_marker());
    }

    // On the first successful admit after a shed run, write the exact dropout_record
    // BEFORE the record that resumed capture so the gap is quantified, never silent.
    void surface_dropout()
    {
        if(!m_dropouts.pending())
            return;
        const dropout_record dr = m_dropouts.harvest(m_clock());
        m_ring.try_push(m_writer.dropout(dr));
    }

    byte_sink           &m_sink;
    record_stream_writer m_writer;
    byte_ring            m_ring;
    dropout_run          m_dropouts;
    clock_fn             m_clock;
    std::size_t          m_drain_batch;
    std::uint32_t        m_records{0};
};

}

#endif
