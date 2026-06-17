#include "plexus/tools/flat_to_mcap.h"

#include "plexus/io/recording/record_decode.h"
#include "plexus/io/recording/record_envelope.h"
#include "plexus/io/recording/record_stream_reader.h"

// The compression-disable macros arrive from plexus_mcap_dep; this is the single
// TU that pulls in the mcap implementation (writer for the transcode, reader so a
// linked round-trip consumer needs no second implementation TU).
#define MCAP_IMPLEMENTATION
#include <mcap/writer.hpp>
#include <mcap/reader.hpp>

#include <map>
#include <array>
#include <string>
#include <vector>
#include <cstdint>
#include <utility>
#include <unordered_map>

namespace plexus::tools {

namespace {

namespace rec = plexus::io::recording;

// The MCAP message/channel encoding names: the transcode never decodes a sample,
// so the payload is opaque plexus bytes. The control-plane and wire-meta channels
// carry a small fixed JSON shape this tool encodes from the decoded scalar fields.
constexpr const char *k_payload_encoding = "plexus/opaque";
constexpr const char *k_events_encoding  = "json";
constexpr const char *k_wire_encoding    = "plexus/wire-frame";

// MCAP rule (the one Foxglove enforces): a "json" message channel must reference a
// schema encoded as "jsonschema" (a JSON Schema document) or no schema at all — never
// a schema whose own encoding is "json". The two synthesized-JSON channels below carry
// a real jsonschema describing the fixed object shapes events_json()/wire_json() emit.
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

std::string hex16(std::uint64_t v)
{
    static constexpr char digits[] = "0123456789abcdef";
    std::string           s(16, '0');
    for(int i = 0; i < 16; ++i)
        s[15 - i] = digits[(v >> (4 * i)) & 0xf];
    return s;
}

std::string topic_for_hash(std::uint64_t topic_hash, const std::string &name)
{
    return name.empty() ? ("plexus/topic/" + hex16(topic_hash)) : name;
}

const char *events_topic_for(rec::record_category cat)
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

std::string peer_hex(const plexus::node_id &peer)
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
std::string events_json(const rec::decoded_record &r)
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

std::string wire_json(const rec::decoded_record &r)
{
    std::string j = "{";
    j += "\"capture_ts\":" + std::to_string(r.capture_ts);
    j += ",\"direction\":" + std::to_string(static_cast<unsigned>(r.wire_dir));
    j += ",\"wire_seq\":" + std::to_string(r.wire_seq);
    j += ",\"peer\":\"" + peer_hex(r.peer) + "\"";
    j += "}";
    return j;
}

// Join topic_hash -> type name from the preamble schema (keyed by type_id) and from
// the endpoint/qos_change records that carry both topic_hash and an optional type_id.
// At HEAD the preamble schema is empty and samples carry no type_id, so the endpoint
// fqn is the load-bearing name source; the schema table is consulted when populated.
std::unordered_map<std::uint64_t, std::string>
build_topic_names(const rec::stream_definitions          &defs,
                  const std::vector<rec::decoded_record> &records)
{
    std::unordered_map<std::uint64_t, std::string> by_type_id;
    for(const auto &[id, name] : defs.schema)
        by_type_id.emplace(id, name);

    std::unordered_map<std::uint64_t, std::string> by_topic;
    for(const auto &r : records)
    {
        const bool is_decl = r.category == rec::record_category::endpoint ||
                             r.category == rec::record_category::qos_change;
        if(!is_decl)
            continue;
        std::string name;
        if(r.type_id)
        {
            const auto it = by_type_id.find(*r.type_id);
            if(it != by_type_id.end())
                name = it->second;
        }
        if(name.empty() && !r.fqn.empty())
            name = r.fqn;
        if(!name.empty())
            by_topic[r.topic_hash] = std::move(name);
    }
    return by_topic;
}

std::span<const std::byte> as_bytes(const std::string &s)
{
    return std::as_bytes(std::span{s.data(), s.size()});
}

// The schema to attach to a channel, or an empty encoding to attach none (schemaId 0 —
// the serializer-agnostic opaque path Foxglove leaves undecoded).
struct channel_schema
{
    std::string_view name;
    std::string_view encoding;
    std::string_view data;
};

// Lazily creates one channel per sample topic, one per control-plane category, and the
// wire/wire-meta pair, then writes each decoded record onto its channel. Every payload is
// copied out of the reader-aliasing span before write (the span borrows the flat stream).
class mcap_emitter
{
public:
    mcap_emitter(mcap::McapWriter &writer, transcode_result &result,
                 const std::unordered_map<std::uint64_t, std::string> &names)
        : m_writer(writer), m_result(result), m_names(names) {}

    void emit_sample(const rec::decoded_record &r)
    {
        auto it = m_sample_channels.find(r.topic_hash);
        if(it == m_sample_channels.end())
        {
            const auto        n  = m_names.find(r.topic_hash);
            const std::string nm = n == m_names.end() ? std::string{} : n->second;
            const std::string topic = topic_for_hash(r.topic_hash, nm);
            const auto        cid   = open_channel(topic, k_payload_encoding, {});
            it = m_sample_channels.emplace(r.topic_hash, cid).first;
        }
        write(it->second, static_cast<std::uint32_t>(r.publication_sequence),
              r.capture_ts, r.source_timestamp, r.payload);
    }

    void emit_wire(const rec::decoded_record &r)
    {
        if(!m_wire_open)
        {
            m_wire_channel      = open_channel("plexus/wire", k_wire_encoding, {});
            m_wire_meta_channel = open_channel("plexus/wire/meta", k_events_encoding,
                                               {k_wire_meta_schema_name, k_jsonschema_encoding,
                                                k_wire_meta_schema});
            m_wire_open         = true;
        }
        // The framed bytes are the message data (the fidelity capture); the join keys
        // (dir/seq/peer) ride a companion message on the meta channel so the cross-node
        // loss join survives the round-trip without decoding the bytes.
        write(m_wire_channel, static_cast<std::uint32_t>(r.wire_seq),
              r.capture_ts, r.capture_ts, r.payload);
        write(m_wire_meta_channel, static_cast<std::uint32_t>(r.wire_seq),
              r.capture_ts, r.capture_ts, as_bytes(wire_json(r)));
    }

    void emit_event(const rec::decoded_record &r)
    {
        auto it = m_event_channels.find(r.category);
        if(it == m_event_channels.end())
        {
            const auto cid = open_channel(events_topic_for(r.category), k_events_encoding,
                                          {k_event_schema_name, k_jsonschema_encoding,
                                           k_event_schema});
            it = m_event_channels.emplace(r.category, cid).first;
        }
        write(it->second, 0, r.capture_ts, r.capture_ts, as_bytes(events_json(r)));
    }

private:
    mcap::ChannelId open_channel(std::string_view topic, std::string_view encoding,
                                 const channel_schema &schema)
    {
        mcap::SchemaId schema_id = 0;
        if(!schema.encoding.empty())
        {
            mcap::Schema s{schema.name, schema.encoding, schema.data};
            m_writer.addSchema(s);
            ++m_result.schemas;
            schema_id = s.id;
        }
        mcap::Channel channel{topic, encoding, schema_id};
        m_writer.addChannel(channel);
        ++m_result.channels;
        return channel.id;
    }

    void write(mcap::ChannelId channel, std::uint32_t seq, std::uint64_t log_time,
               std::uint64_t publish_time, std::span<const std::byte> bytes)
    {
        m_scratch.assign(bytes.begin(), bytes.end());
        mcap::Message msg;
        msg.channelId   = channel;
        msg.sequence    = seq;
        msg.logTime     = log_time;
        msg.publishTime = publish_time;
        msg.dataSize    = m_scratch.size();
        msg.data        = m_scratch.data();
        if(m_writer.write(msg).ok())
            ++m_result.messages;
    }

    mcap::McapWriter &m_writer;
    transcode_result &m_result;
    const std::unordered_map<std::uint64_t, std::string> &m_names;

    std::unordered_map<std::uint64_t, mcap::ChannelId> m_sample_channels;
    std::map<rec::record_category, mcap::ChannelId>    m_event_channels;
    std::vector<std::byte>                             m_scratch;
    mcap::ChannelId                                    m_wire_channel{0};
    mcap::ChannelId                                    m_wire_meta_channel{0};
    bool                                               m_wire_open{false};
};

void emit_record(mcap_emitter &emitter, const rec::decoded_record &r)
{
    switch(r.category)
    {
        case rec::record_category::sample:     emitter.emit_sample(r); break;
        case rec::record_category::wire_frame: emitter.emit_wire(r); break;
        case rec::record_category::drop:
        case rec::record_category::qos_change:
        case rec::record_category::participant:
        case rec::record_category::endpoint:
        case rec::record_category::security:
        case rec::record_category::dropout:    emitter.emit_event(r); break;
        default:                               break;
    }
}

}

transcode_result flat_to_mcap(std::span<const std::byte>   flat_stream,
                              const std::filesystem::path &out_mcap)
{
    transcode_result out;

    rec::record_stream_reader reader{flat_stream};
    rec::stream_definitions   defs;
    if(!reader.read_definitions(defs))
    {
        out.error = "not a plexus flat record-stream (bad header/preamble)";
        return out;
    }

    std::vector<rec::decoded_record> records;
    const rec::recovery_result       rr = reader.recover(records);
    out.recovered                = rr.recovered;
    out.trailing_partial_dropped = rr.trailing_partial_dropped;
    out.corruption_skipped       = rr.corruption_skipped;

    mcap::McapWriterOptions wopts{"plexus"};
    wopts.noChunking  = true;
    wopts.compression = mcap::Compression::None;

    mcap::McapWriter writer;
    if(const auto status = writer.open(out_mcap.string(), wopts); !status.ok())
    {
        out.error = "could not open MCAP output: " + status.message;
        return out;
    }

    const auto   topic_names = build_topic_names(defs, records);
    mcap_emitter emitter{writer, out, topic_names};
    for(const auto &r : records)
        emit_record(emitter, r);

    writer.close();
    out.ok = true;
    return out;
}

}
