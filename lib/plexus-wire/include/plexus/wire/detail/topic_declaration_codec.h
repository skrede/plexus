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

inline void write_string(writer &w, const std::string &s)
{
    w.u16(static_cast<std::uint16_t>(s.size()));
    w.bytes(std::as_bytes(std::span<const char>{s.data(), s.size()}));
}

inline std::vector<std::byte> encode_topic_declaration(const topic_declaration &td)
{
    std::vector<std::byte> buf(detail::topic_declaration_min_size + td.fqn.size() + td.type_name.size());
    writer w{buf};
    w.u64(td.topic_hash);
    w.u64(td.type_id);
    w.u8(static_cast<std::uint8_t>(td.state));
    write_string(w, td.fqn);
    write_string(w, td.type_name);
    return buf;
}

// Decode off an untrusted session frame. The whole field list is read against the latching reader
// and ok() is tested once: a truncated frame, a string past its lid, or a state byte outside the
// closed three-state set yields nullopt with no partial struct.
inline std::optional<topic_declaration> decode_topic_declaration(std::span<const std::byte> payload)
{
    reader r{payload};
    topic_declaration td{};
    td.topic_hash    = r.u64();
    td.type_id       = r.u64();
    const auto state = r.u8();
    const auto fqn   = r.length_prefixed<std::uint16_t>();
    const auto name  = r.length_prefixed<std::uint16_t>();

    if(!r.ok() || fqn.size() > detail::k_max_fqn || name.size() > detail::k_max_type_name || state > static_cast<std::uint8_t>(type_state::declared))
        return std::nullopt;

    td.state = static_cast<type_state>(state);
    td.fqn.assign(reinterpret_cast<const char *>(fqn.data()), fqn.size());
    td.type_name.assign(reinterpret_cast<const char *>(name.data()), name.size());
    return td;
}

}

#endif
