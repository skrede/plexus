#ifndef HPP_GUARD_PLEXUS_IO_RECORDING_RECORD_DECODE_H
#define HPP_GUARD_PLEXUS_IO_RECORDING_RECORD_DECODE_H

#include "plexus/io/capture_policy.h"
#include "plexus/io/recording/wire_record.h"
#include "plexus/io/recording/record_envelope.h"

#include "plexus/wire/cursor.h"

#include "plexus/node_id.h"

#include <span>
#include <string>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace plexus::io::recording {

struct decoded_record
{
    record_category category{record_category::sample};
    std::uint64_t capture_ts{};
    std::uint64_t topic_hash{};
    std::uint64_t publication_sequence{};
    std::uint64_t source_timestamp{};
    std::uint64_t reception_timestamp{};
    std::optional<std::uint64_t> type_id{};
    capture_fidelity fidelity{capture_fidelity::off};
    std::uint8_t edge{};
    std::uint8_t verdict{};
    std::uint64_t count{};
    node_id peer{};
    std::string fqn;
    std::span<const std::byte> payload{};
    wire_direction wire_dir{wire_direction::out};
    std::uint64_t wire_seq{};
};

namespace detail {

inline void decode_sample(wire::reader &r, decoded_record &rec)
{
    rec.capture_ts           = r.u64();
    rec.topic_hash           = r.u64();
    rec.publication_sequence = r.u64();
    rec.source_timestamp     = r.u64();
    rec.reception_timestamp  = r.u64();
    const bool present       = r.u8() != 0;
    const std::uint64_t id   = r.varint().value_or(0);
    if(present)
        rec.type_id = id;
    rec.fidelity             = static_cast<capture_fidelity>(r.u8());
    const std::uint64_t plen = r.varint().value_or(0);
    rec.payload              = r.bytes(static_cast<std::size_t>(plen));
}

inline void decode_drop(wire::reader &r, decoded_record &rec)
{
    rec.capture_ts          = r.u64();
    rec.edge                = r.u8();
    const std::uint8_t band = r.u8();
    (void)band;
    rec.topic_hash = r.u64();
    rec.count      = r.u64();
}

inline void decode_qos(wire::reader &r, decoded_record &rec)
{
    rec.capture_ts         = r.u64();
    rec.edge               = r.u8();
    rec.topic_hash         = r.u64();
    rec.verdict            = r.u8();
    const bool present     = r.u8() != 0;
    const std::uint64_t id = r.varint().value_or(0);
    if(present)
        rec.type_id = id;
}

inline void decode_participant(wire::reader &r, decoded_record &rec)
{
    rec.capture_ts = r.u64();
    rec.edge       = r.u8();
    for(std::byte &b : rec.peer)
        b = static_cast<std::byte>(r.u8());
}

inline void decode_endpoint(wire::reader &r, decoded_record &rec)
{
    rec.capture_ts         = r.u64();
    rec.edge               = r.u8();
    rec.topic_hash         = r.u64();
    const bool present     = r.u8() != 0;
    const std::uint64_t id = r.varint().value_or(0);
    if(present)
        rec.type_id = id;
    const std::uint64_t flen = r.varint().value_or(0);
    const auto name          = r.bytes(static_cast<std::size_t>(flen));
    rec.fqn.assign(reinterpret_cast<const char *>(name.data()), name.size());
}

inline void decode_security(wire::reader &r, decoded_record &rec)
{
    rec.capture_ts = r.u64();
    rec.edge       = r.u8();
    rec.verdict    = r.u8();
    for(std::byte &b : rec.peer)
        b = static_cast<std::byte>(r.u8());
}

inline void decode_dropout(wire::reader &r, decoded_record &rec)
{
    rec.capture_ts            = r.u64();
    rec.count                 = r.varint().value_or(0);
    const std::uint64_t bytes = r.varint().value_or(0);
    (void)bytes;
    rec.fidelity = static_cast<capture_fidelity>(r.u8());
}

inline void decode_wire(wire::reader &r, decoded_record &rec)
{
    rec.capture_ts = r.u64();
    rec.wire_dir   = static_cast<wire_direction>(r.u8());
    rec.wire_seq   = r.u64();
    for(std::byte &b : rec.peer)
        b = static_cast<std::byte>(r.u8());
    const std::uint64_t len = r.varint().value_or(0);
    rec.payload             = r.bytes(static_cast<std::size_t>(len));
}

}

// NOLINTNEXTLINE(readability-function-size)
inline bool decode_record_body(std::span<const std::byte> body, decoded_record &rec)
{
    wire::reader r{body};
    rec.category = static_cast<record_category>(r.u8());
    switch(rec.category)
    {
        case record_category::sample:
            detail::decode_sample(r, rec);
            break;
        case record_category::drop:
            detail::decode_drop(r, rec);
            break;
        case record_category::qos_change:
            detail::decode_qos(r, rec);
            break;
        case record_category::participant:
            detail::decode_participant(r, rec);
            break;
        case record_category::endpoint:
            detail::decode_endpoint(r, rec);
            break;
        case record_category::security:
            detail::decode_security(r, rec);
            break;
        case record_category::dropout:
            detail::decode_dropout(r, rec);
            break;
        case record_category::wire_frame:
            detail::decode_wire(r, rec);
            break;
        default:
            break;
    }
    return r.ok();
}

}

#endif
