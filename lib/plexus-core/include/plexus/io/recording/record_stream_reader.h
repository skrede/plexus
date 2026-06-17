#ifndef HPP_GUARD_PLEXUS_IO_RECORDING_RECORD_STREAM_READER_H
#define HPP_GUARD_PLEXUS_IO_RECORDING_RECORD_STREAM_READER_H

#include "plexus/io/capture_policy.h"
#include "plexus/io/recording/record_decode.h"
#include "plexus/io/recording/record_format.h"
#include "plexus/io/recording/record_envelope.h"

#include "plexus/wire/cursor.h"
#include "plexus/wire/crc32c.h"
#include "plexus/wire/varint.h"

#include "plexus/node_id.h"

#include <span>
#include <string>
#include <vector>
#include <utility>
#include <cstddef>
#include <cstdint>

namespace plexus::io::recording {

// One decoded schema entry: a type id, its human name, and the four opaque self-description
// fields a host projector reads. Owned — the preamble is parsed once, offline.
struct schema_definition
{
    std::uint64_t          type_id{};
    std::string            type_name;
    std::string            message_encoding;
    std::string            schema_name;
    std::string            schema_encoding;
    std::vector<std::byte> schema_data;
};

// The Definitions preamble decoded from a stream head: the node identity, the
// recording-QoS default in force, the opaque schema table a host projector resolves
// codecs from, and the crypto tap position the capture used. Owned — parsed once, offline.
struct stream_definitions
{
    std::uint64_t                  clock_epoch{};
    node_id                        node{};
    topic_capture_rule             rule{};
    std::vector<schema_definition> schema;
    capture_crypto_position        crypto_position{capture_crypto_position::cleartext};
};

// The outcome of a recovery scan over a possibly-truncated/corrupt stream.
struct recovery_result
{
    bool        header_ok{false};
    std::size_t recovered{0};
    bool        trailing_partial_dropped{false};
    bool        corruption_skipped{false};
};

// The offline, host-side reader + crash-recovery scan over the validated wire::reader.
// It never runs on the producer hot path and tolerates an arbitrary truncated/garbage
// tail without UB (every field read is bounds-checked through the latched reader). The
// scan reads the header + preamble, then iterates [varint len][payload] records (the
// byte_ring framing IS the stream framing). The FIRST record whose length overruns the
// remaining bytes is the truncation point — every complete prior record is recovered and
// only the trailing partial is lost. A record whose CRC fails is corruption: the scan
// resyncs forward to the next sync marker and resumes, so a corrupt span loses only its
// own records, not the rest of the stream.
class record_stream_reader
{
public:
    explicit record_stream_reader(std::span<const std::byte> stream) noexcept : m_stream(stream) {}

    bool read_definitions(stream_definitions &out)
    {
        wire::reader r{m_stream};
        if(r.u32() != k_stream_magic)
            return false;
        if(r.u16() != k_format_version)
            return false;
        out.clock_epoch = r.u64();
        for(std::byte &b : out.node)
            b = static_cast<std::byte>(r.u8());
        out.rule.fidelity   = static_cast<capture_fidelity>(r.u8());
        out.rule.mode       = static_cast<decimation_mode>(r.u8());
        out.rule.decimation = static_cast<std::uint32_t>(r.varint().value_or(0));
        out.rule.window_ns  = r.varint().value_or(0);
        const std::uint64_t entries = r.varint().value_or(0);
        out.schema.clear();
        for(std::uint64_t i = 0; i < entries && r.ok(); ++i)
        {
            schema_definition e;
            e.type_id          = r.varint().value_or(0);
            e.type_name        = read_string(r);
            e.message_encoding = read_string(r);
            e.schema_name      = read_string(r);
            e.schema_encoding  = read_string(r);
            e.schema_data      = read_bytes(r);
            out.schema.push_back(std::move(e));
        }
        out.crypto_position = static_cast<capture_crypto_position>(r.u8());
        if(!r.ok())
            return false;
        m_cursor = r.consumed();
        return true;
    }

    // Scan the data section front-to-back, appending every recovered record to `out` and
    // reporting the recovery accounting. Call read_definitions first.
    recovery_result recover(std::vector<decoded_record> &out)
    {
        recovery_result res;
        res.header_ok   = true;
        std::size_t at  = m_cursor;
        while(at < m_stream.size())
        {
            std::size_t len_off = at;
            const auto  len     = wire::read_varint(m_stream, len_off);
            if(!len)
            {
                res.trailing_partial_dropped = true;
                break;
            }
            const std::size_t payload_end = len_off + static_cast<std::size_t>(*len);
            if(payload_end > m_stream.size())
            {
                res.trailing_partial_dropped = true;
                break;
            }
            const std::span<const std::byte> payload = m_stream.subspan(len_off, static_cast<std::size_t>(*len));
            if(is_sync(payload))
            {
                at = payload_end;
                continue;
            }
            decoded_record rec;
            if(!validate_and_decode(payload, rec))
            {
                // The length varint may itself be corrupt, so do NOT trust payload_end:
                // resync by scanning forward for the next sync marker (byte-granular).
                res.corruption_skipped = true;
                at = resync_from(at + 1);
                continue;
            }
            at = payload_end;
            out.push_back(std::move(rec));
            ++res.recovered;
        }
        return res;
    }

private:
    static std::vector<std::byte> read_bytes(wire::reader &r)
    {
        const std::uint64_t len = r.varint().value_or(0);
        const auto          b   = r.bytes(static_cast<std::size_t>(len));
        return {b.begin(), b.end()};
    }

    static std::string read_string(wire::reader &r)
    {
        const std::uint64_t len = r.varint().value_or(0);
        const auto          b   = r.bytes(static_cast<std::size_t>(len));
        return {reinterpret_cast<const char *>(b.data()), b.size()};
    }

    static bool is_sync(std::span<const std::byte> payload) noexcept
    {
        if(payload.size() != sizeof(std::uint32_t))
            return false;
        wire::reader r{payload};
        return r.u32() == k_sync_marker;
    }

    // Scan forward from `from` for the next on-disk sync record ([varint len][marker]) and
    // return the offset at which it begins; returns the stream end if none remains. The
    // main loop then re-reads that offset as a sync record and resumes past it.
    [[nodiscard]] std::size_t resync_from(std::size_t from) const noexcept
    {
        for(std::size_t at = from; at < m_stream.size(); ++at)
        {
            std::size_t off = at;
            const auto  len = wire::read_varint(m_stream, off);
            if(!len || off + static_cast<std::size_t>(*len) > m_stream.size())
                continue;
            if(is_sync(m_stream.subspan(off, static_cast<std::size_t>(*len))))
                return at;
        }
        return m_stream.size();
    }

    static bool validate_and_decode(std::span<const std::byte> payload, decoded_record &rec)
    {
        if(payload.size() < sizeof(std::uint32_t))
            return false;
        const std::size_t   body_len = payload.size() - sizeof(std::uint32_t);
        wire::reader        cr{payload.subspan(body_len)};
        const std::uint32_t stored = cr.u32();
        if(wire::crc32c(payload.first(body_len)) != stored)
            return false;
        return decode_record_body(payload.first(body_len), rec);
    }

    std::span<const std::byte> m_stream;
    std::size_t                m_cursor{0};
};

}

#endif
