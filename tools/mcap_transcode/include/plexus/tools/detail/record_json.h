#ifndef HPP_GUARD_PLEXUS_TOOLS_DETAIL_RECORD_JSON_H
#define HPP_GUARD_PLEXUS_TOOLS_DETAIL_RECORD_JSON_H

#include "plexus/io/recording/record_decode.h"
#include "plexus/io/recording/record_envelope.h"

#include <span>
#include <string>
#include <cstddef>
#include <cstdint>

namespace plexus::tools::detail {

namespace rec = plexus::io::recording;

// The MCAP message/channel encoding names: the transcode never decodes a sample,
// so the payload is opaque plexus bytes. The control-plane and wire-meta channels
// carry a small fixed JSON shape this tool encodes from the decoded scalar fields.
constexpr const char *k_payload_encoding = "plexus/opaque";
constexpr const char *k_events_encoding  = "json";
constexpr const char *k_wire_encoding    = "plexus/wire-frame";

// The mcap-doctor rule a compliant viewer enforces: a "json" message channel must
// reference a schema encoded as "jsonschema" (a JSON Schema document) or no schema at
// all — never a schema whose own encoding is "json". The two synthesized-JSON channels
// below carry a real jsonschema describing the fixed object shapes events_json()/wire_json() emit.
constexpr const char *k_jsonschema_encoding = "jsonschema";

constexpr const char *k_event_schema_name = "plexus.event";
constexpr const char *k_event_schema =
    R"({"type":"object","title":"plexus.event","properties":{)"
    R"("category":{"type":"integer"},"capture_ts":{"type":"integer"},)"
    R"("topic_hash":{"type":"integer"},"edge":{"type":"integer"},)"
    R"("verdict":{"type":"integer"},"count":{"type":"integer"},)"
    R"("peer":{"type":"string"},"fqn":{"type":"string"},"type_id":{"type":"integer"}}})";

constexpr const char *k_wire_meta_schema_name = "plexus.wire.meta";
constexpr const char *k_wire_meta_schema =
    R"({"type":"object","title":"plexus.wire.meta","properties":{)"
    R"("capture_ts":{"type":"integer"},"direction":{"type":"integer"},)"
    R"("wire_seq":{"type":"integer"},"peer":{"type":"string"}}})";

inline std::string hex16(std::uint64_t v)
{
    static constexpr char digits[] = "0123456789abcdef";
    std::string           s(16, '0');
    for(int i = 0; i < 16; ++i)
        s[15 - i] = digits[(v >> (4 * i)) & 0xf];
    return s;
}

inline std::string topic_for_hash(std::uint64_t topic_hash, const std::string &name)
{
    return name.empty() ? ("plexus/topic/" + hex16(topic_hash)) : name;
}

inline const char *events_topic_for(rec::record_category cat)
{
    switch(cat)
    {
        case rec::record_category::drop:        return "plexus/events/drop";
        case rec::record_category::qos_change:  return "plexus/events/qos";
        case rec::record_category::participant: return "plexus/events/participant";
        case rec::record_category::endpoint:    return "plexus/events/endpoint";
        case rec::record_category::security:    return "plexus/events/security";
        case rec::record_category::dropout:     return "plexus/events/dropout";
        default:                                return "plexus/events/other";
    }
}

inline std::string peer_hex(const plexus::node_id &peer)
{
    static constexpr char digits[] = "0123456789abcdef";
    std::string           s;
    s.reserve(peer.size() * 2);
    for(std::byte b : peer)
    {
        const auto v = static_cast<unsigned>(b);
        s.push_back(digits[(v >> 4) & 0xf]);
        s.push_back(digits[v & 0xf]);
    }
    return s;
}

// A small JSON object for a control-plane event record. Only the scalar facts the
// decoder recovered are emitted; the bytes of a sample are never reached here.
inline std::string events_json(const rec::decoded_record &r)
{
    std::string j = "{";
    j += "\"category\":" + std::to_string(static_cast<unsigned>(r.category));
    j += ",\"capture_ts\":" + std::to_string(r.capture_ts);
    j += ",\"topic_hash\":" + std::to_string(r.topic_hash);
    j += ",\"edge\":" + std::to_string(static_cast<unsigned>(r.edge));
    j += ",\"verdict\":" + std::to_string(static_cast<unsigned>(r.verdict));
    j += ",\"count\":" + std::to_string(r.count);
    j += ",\"peer\":\"" + peer_hex(r.peer) + "\"";
    if(!r.fqn.empty())
        j += ",\"fqn\":\"" + r.fqn + "\"";
    if(r.type_id)
        j += ",\"type_id\":" + std::to_string(*r.type_id);
    j += "}";
    return j;
}

inline std::string wire_json(const rec::decoded_record &r)
{
    std::string j = "{";
    j += "\"capture_ts\":" + std::to_string(r.capture_ts);
    j += ",\"direction\":" + std::to_string(static_cast<unsigned>(r.wire_dir));
    j += ",\"wire_seq\":" + std::to_string(r.wire_seq);
    j += ",\"peer\":\"" + peer_hex(r.peer) + "\"";
    j += "}";
    return j;
}

inline std::span<const std::byte> as_bytes(const std::string &s)
{
    return std::as_bytes(std::span{s.data(), s.size()});
}

}

#endif
