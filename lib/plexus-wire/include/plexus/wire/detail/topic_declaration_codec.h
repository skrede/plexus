#ifndef HPP_GUARD_PLEXUS_WIRE_DETAIL_TOPIC_DECLARATION_CODEC_H
#define HPP_GUARD_PLEXUS_WIRE_DETAIL_TOPIC_DECLARATION_CODEC_H

#include "plexus/wire/reader.h"
#include "plexus/wire/writer.h"
#include "plexus/wire/topic_declaration.h"

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace plexus::wire {

inline std::vector<std::byte> encode_topic_declaration(const topic_declaration &td)
{
    std::vector<std::byte> buf(detail::topic_declaration_min_size + td.type_name.size());
    writer w{buf};
    w.u64(td.topic_hash);
    w.u64(td.type_id);
    w.u8(static_cast<std::uint8_t>(td.state));
    w.u16(static_cast<std::uint16_t>(td.type_name.size()));
    w.bytes(std::as_bytes(std::span<const char>{td.type_name.data(), td.type_name.size()}));
    return buf;
}

// Decode off an untrusted session frame. The whole field list is read against the latching reader
// and ok() is tested once: a truncated frame, a name past the lid, or a state byte outside the
// closed three-state set yields nullopt with no partial struct.
inline std::optional<topic_declaration> decode_topic_declaration(std::span<const std::byte> payload)
{
    reader r{payload};
    topic_declaration td{};
    td.topic_hash    = r.u64();
    td.type_id       = r.u64();
    const auto state = r.u8();
    const auto name  = r.length_prefixed<std::uint16_t>();

    if(!r.ok() || name.size() > detail::k_max_type_name || state > static_cast<std::uint8_t>(type_state::declared))
        return std::nullopt;

    td.state = static_cast<type_state>(state);
    td.type_name.assign(reinterpret_cast<const char *>(name.data()), name.size());
    return td;
}

}

#endif
