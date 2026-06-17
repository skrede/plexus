#ifndef HPP_GUARD_PLEXUS_IO_RECORDING_RECORD_STREAM_WRITER_H
#define HPP_GUARD_PLEXUS_IO_RECORDING_RECORD_STREAM_WRITER_H

#include "plexus/io/message_info.h"
#include "plexus/io/capture_policy.h"
#include "plexus/io/security_event.h"
#include "plexus/io/observation_events.h"
#include "plexus/io/detail/drop_event.h"
#include "plexus/io/recording/record_format.h"
#include "plexus/io/recording/dropout_record.h"
#include "plexus/io/recording/record_envelope.h"

#include "plexus/wire/cursor.h"
#include "plexus/wire/crc32c.h"

#include "plexus/node_id.h"

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace plexus::io::recording {

// One entry of the Definitions preamble's offline-decode key: a topic/type id mapped
// to its human type name. A host decoder reads this table to resolve the codec for the
// raw bytes a sample carries — the stream itself never holds a codec.
struct type_schema_entry
{
    std::uint64_t    type_id{};
    std::string_view type_name{};
};

// Encodes the flat append-only record stream into a caller-supplied scratch and returns
// each piece as a contiguous span the caller frames (the byte_ring's [varint len][bytes]
// IS the stream's per-record length prefix, so the writer emits the record PAYLOAD and
// never re-frames). begin_stream emits the header (magic + format version + capture-clock
// epoch) and the Definitions preamble (node identity + the recording-QoS in force + the
// type_id->type_name table). Each record payload is [u8 category][fields...][crc32c],
// the CRC-32C covering the category+fields so a recovery scan validates each record and
// drops only a corrupt/truncated tail. A separately-pushed sync_marker record lets the
// scan resynchronize the record boundary after skipping a corrupt span. Every field goes
// through the reused wire::writer cursor — no hand-rolled offsets. The scratch is grown
// ONCE and reused across records, so the steady-state encode allocates nothing.
class record_stream_writer
{
public:
    explicit record_stream_writer(std::size_t scratch_bytes = 64u * 1024u)
        : m_scratch(scratch_bytes)
    {
    }

    std::span<const std::byte> begin_stream(std::uint64_t clock_epoch,
                                            const node_id &node,
                                            topic_capture_rule rule,
                                            std::span<const type_schema_entry> schema)
    {
        wire::writer w{m_scratch};
        w.u32(k_stream_magic);
        w.u16(k_format_version);
        w.u64(clock_epoch);
        for(std::byte b : node)
            w.u8(std::to_integer<std::uint8_t>(b));
        w.u8(static_cast<std::uint8_t>(rule.fidelity));
        w.u8(static_cast<std::uint8_t>(rule.mode));
        w.varint(rule.decimation);
        w.varint(rule.window_ns);
        w.varint(schema.size());
        for(const type_schema_entry &e : schema)
        {
            w.varint(e.type_id);
            w.varint(e.type_name.size());
            w.bytes(as_bytes(e.type_name));
        }
        return {m_scratch.data(), w.offset()};
    }

    // The sync marker and the dropout record encode into a SEPARATE small scratch, never
    // the record scratch: the recorder emits them around a record whose bytes already sit
    // in the record scratch (an alias the main scratch must not clobber mid-admit).
    std::span<const std::byte> sync_marker()
    {
        wire::writer w{m_aux};
        w.u32(k_sync_marker);
        return {m_aux.data(), w.offset()};
    }

    // A sample (a captured message): topic identity + the metadata floor + an optional
    // type_id + the raw payload bytes at the recorded fidelity. A metadata-only record
    // passes an empty payload; the encoder never invokes a codec — payload is opaque.
    std::span<const std::byte> sample(std::uint64_t capture_ts,
                                      std::uint64_t topic_hash,
                                      const message_info &info,
                                      std::uint64_t type_id,
                                      bool type_id_present,
                                      capture_fidelity fidelity,
                                      std::span<const std::byte> payload)
    {
        wire::writer w{m_scratch};
        w.u8(static_cast<std::uint8_t>(record_category::sample));
        w.u64(capture_ts);
        w.u64(topic_hash);
        w.u64(info.publication_sequence);
        w.u64(info.source_timestamp);
        w.u64(info.reception_timestamp);
        w.u8(type_id_present ? 1u : 0u);
        w.varint(type_id_present ? type_id : 0u);
        w.u8(static_cast<std::uint8_t>(fidelity));
        w.varint(payload.size());
        w.bytes(payload);
        return seal(w.offset());
    }

    std::span<const std::byte> drop(std::uint64_t capture_ts, const io::detail::drop_event &e)
    {
        wire::writer w{m_scratch};
        w.u8(static_cast<std::uint8_t>(record_category::drop));
        w.u64(capture_ts);
        w.u8(static_cast<std::uint8_t>(e.cause));
        w.u8(e.band);
        w.u64(e.topic_hash);
        w.u64(e.count);
        return seal(w.offset());
    }

    std::span<const std::byte> qos_change(std::uint64_t capture_ts, const qos_change_event &e)
    {
        wire::writer w{m_scratch};
        w.u8(static_cast<std::uint8_t>(record_category::qos_change));
        w.u64(capture_ts);
        w.u8(static_cast<std::uint8_t>(e.edge));
        w.u64(e.topic_hash);
        w.u8(static_cast<std::uint8_t>(e.verdict));
        w.u8(e.type_id ? 1u : 0u);
        w.varint(e.type_id ? *e.type_id : 0u);
        return seal(w.offset());
    }

    std::span<const std::byte> participant(std::uint64_t capture_ts, const participant_event &e)
    {
        wire::writer w{m_scratch};
        w.u8(static_cast<std::uint8_t>(record_category::participant));
        w.u64(capture_ts);
        w.u8(static_cast<std::uint8_t>(e.edge));
        for(std::byte b : e.self)
            w.u8(std::to_integer<std::uint8_t>(b));
        return seal(w.offset());
    }

    std::span<const std::byte> endpoint(std::uint64_t capture_ts,
                                        std::string_view fqn,
                                        const endpoint_event &e)
    {
        wire::writer w{m_scratch};
        w.u8(static_cast<std::uint8_t>(record_category::endpoint));
        w.u64(capture_ts);
        w.u8(static_cast<std::uint8_t>(e.edge));
        w.u64(e.topic_hash);
        w.u8(e.type_id ? 1u : 0u);
        w.varint(e.type_id ? *e.type_id : 0u);
        w.varint(fqn.size());
        w.bytes(as_bytes(fqn));
        return seal(w.offset());
    }

    std::span<const std::byte> security(std::uint64_t capture_ts, const security_event &e)
    {
        wire::writer w{m_scratch};
        w.u8(static_cast<std::uint8_t>(record_category::security));
        w.u64(capture_ts);
        w.u8(static_cast<std::uint8_t>(e.kind));
        w.u8(static_cast<std::uint8_t>(e.cause));
        for(std::byte b : e.peer)
            w.u8(std::to_integer<std::uint8_t>(b));
        return seal(w.offset());
    }

    std::span<const std::byte> dropout(const dropout_record &r)
    {
        wire::writer w{m_aux};
        w.u8(static_cast<std::uint8_t>(record_category::dropout));
        w.u64(r.ts);
        w.varint(r.count);
        w.varint(r.bytes);
        w.u8(static_cast<std::uint8_t>(r.max_fidelity));
        return seal_in(m_aux, w.offset());
    }

private:
    static std::span<const std::byte> as_bytes(std::string_view s) noexcept
    {
        return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
    }

    // Append the CRC-32C over the [category][fields] already written at the record scratch
    // front and return the contiguous [body][crc] record payload. The caller frames it (the
    // ring varint), so this is the recover-validated unit: the scan recomputes the CRC over
    // the payload minus its trailing 4 bytes.
    std::span<const std::byte> seal(std::size_t body_len) { return seal_in(m_scratch, body_len); }

    static std::span<const std::byte> seal_in(std::vector<std::byte> &buf, std::size_t body_len)
    {
        const std::uint32_t crc = wire::crc32c({buf.data(), body_len});
        wire::writer        cw{{buf.data() + body_len, sizeof(std::uint32_t)}};
        cw.u32(crc);
        return {buf.data(), body_len + sizeof(std::uint32_t)};
    }

    std::vector<std::byte> m_scratch;
    std::vector<std::byte> m_aux = std::vector<std::byte>(256u);
};

}

#endif
