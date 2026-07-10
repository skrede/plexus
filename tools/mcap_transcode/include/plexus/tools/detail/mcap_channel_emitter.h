#ifndef HPP_GUARD_PLEXUS_TOOLS_DETAIL_MCAP_CHANNEL_EMITTER_H
#define HPP_GUARD_PLEXUS_TOOLS_DETAIL_MCAP_CHANNEL_EMITTER_H

#include "plexus/tools/detail/record_json.h"
#include "plexus/tools/detail/schema_resolution.h"

#include "plexus/tools/mcap_schema.h"
#include "plexus/tools/flat_to_mcap.h"

#include <mcap/writer.hpp>

#include <map>
#include <span>
#include <string>
#include <vector>
#include <cstdint>
#include <optional>
#include <string_view>
#include <unordered_map>

namespace plexus::tools::detail {

// Lazily creates one channel per sample topic, one per control-plane category, and the
// wire/wire-meta pair, then writes each decoded record onto its channel. Every payload is
// copied out of the reader-aliasing span before write (the span borrows the flat stream).
class mcap_emitter
{
public:
    mcap_emitter(mcap::McapWriter &writer, transcode_result &result,
                 const std::unordered_map<std::uint64_t, std::string>        &names,
                 const std::unordered_map<std::uint64_t, declared_label>     &labels,
                 const std::unordered_map<std::uint64_t, std::uint64_t>      &hints,
                 schema_provider &provider, hint_translator &translate)
        : m_writer(writer), m_result(result), m_names(names), m_labels(labels),
          m_hints(hints), m_provider(provider), m_translate(translate) {}

    void emit_sample(const rec::decoded_record &r)
    {
        auto it = m_sample_channels.find(r.topic_hash);
        if(it == m_sample_channels.end())
        {
            const auto        n  = m_names.find(r.topic_hash);
            const std::string nm = n == m_names.end() ? std::string{} : n->second;
            const std::string topic = topic_for_hash(r.topic_hash, nm);
            const auto        cid   = open_sample_channel(topic, r);
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
    // Resolve the sample's type_id to a schema; a resolved schema opens its channel with that
    // message_encoding + schema, an unresolved type falls back to the opaque path
    // (plexus/opaque, schemaId 0).
    mcap::ChannelId open_sample_channel(std::string_view topic, const rec::decoded_record &r)
    {
        if(r.type_id)
            if(const auto s = resolve_schema(*r.type_id))
                return open_channel(topic, s->message_encoding,
                                    {s->schema_name, s->schema_encoding, s->schema_data});
        return open_channel(topic, k_payload_encoding, {});
    }

    // Resolution order: the consumer provider (keyed on type_id, wins on conflict), then the
    // hint-translator applied to the type's recorded schema_hint, then the preamble-declared
    // label. Any source returning nullopt defers to the next; all three empty => opaque.
    std::optional<mcap_schema> resolve_schema(std::uint64_t type_id)
    {
        if(m_provider)
            if(auto s = m_provider(type_id))
                return s;
        if(m_translate)
        {
            const auto h = m_hints.find(type_id);
            if(h != m_hints.end() && h->second)
                if(auto s = m_translate(h->second))
                    return s;
        }
        const auto it = m_labels.find(type_id);
        if(it != m_labels.end())
        {
            const declared_label &d = it->second;
            return mcap_schema{d.message_encoding, d.schema_name, d.schema_encoding, d.schema_data};
        }
        return std::nullopt;
    }

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
    const std::unordered_map<std::uint64_t, std::string>    &m_names;
    const std::unordered_map<std::uint64_t, declared_label> &m_labels;
    const std::unordered_map<std::uint64_t, std::uint64_t>  &m_hints;
    schema_provider &m_provider;
    hint_translator &m_translate;

    std::unordered_map<std::uint64_t, mcap::ChannelId> m_sample_channels;
    std::map<rec::record_category, mcap::ChannelId>    m_event_channels;
    std::vector<std::byte>                             m_scratch;
    mcap::ChannelId                                    m_wire_channel{0};
    mcap::ChannelId                                    m_wire_meta_channel{0};
    bool                                               m_wire_open{false};
};

inline void emit_record(mcap_emitter &emitter, const rec::decoded_record &r)
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

#endif
