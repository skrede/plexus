#ifndef HPP_GUARD_PLEXUS_EXAMPLE_TOPIC_GRAPH_TYPES_H
#define HPP_GUARD_PLEXUS_EXAMPLE_TOPIC_GRAPH_TYPES_H

// The contract both ends of the topic-propagation example share: the topics they produce, the type
// identities they declare, and the lookup each runs against its own enumeration surface. It is one
// header on purpose — the two programs sit on opposite ends of a real link, and the id (the
// equality token a subscriber matches a publisher on) and the name (what the enumeration carries
// across) only mean anything if both spell them identically.

#include "plexus/graph/topic_record.h"

#include "plexus/match/key_pattern.h"

#include "plexus/expected.h"
#include "plexus/wire_bytes.h"
#include "plexus/typed_codec.h"

#include <span>
#include <array>
#include <memory>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>
#include <system_error>

namespace example {

// One topic per producer, so each end has something the other must enumerate by name AND by
// declared type. The report topic is untyped: it carries the device's own enumeration verdict back
// to the host gate as text, not a modelled value.
inline constexpr std::string_view k_device_topic = "telemetry";
inline constexpr std::string_view k_host_topic   = "command";
inline constexpr std::string_view k_report_topic = "graph";

struct counter
{
    std::uint32_t value;
};

struct reading_type
{
    static constexpr std::uint64_t id   = 0x0B0A0D70ull;
    static constexpr std::string_view name = "reading";
};

struct command_type
{
    static constexpr std::uint64_t id   = 0x0B0AC0FFull;
    static constexpr std::string_view name = "command";
};

// One little-endian counter codec whose declared identity comes from the tag: the two topics carry
// the same four bytes under two DISTINCT type identities, which is what lets the gate prove the
// right name reached the right topic rather than merely that some name reached something.
template<typename Tag>
struct counter_codec
{
    using value_type = counter;

    plexus::wire_bytes<> encode(const counter &v) const
    {
        auto owner = std::make_shared<std::array<std::byte, 4>>();
        for(int i = 0; i < 4; ++i)
            (*owner)[i] = static_cast<std::byte>((v.value >> (8 * i)) & 0xff);
        std::span<const std::byte> view{owner->data(), owner->size()};
        return plexus::wire_bytes<>{view, std::move(owner)};
    }

    plexus::expected<void, std::error_code> decode(std::span<const std::byte> bytes, counter &out) const
    {
        if(bytes.size() != 4)
            return plexus::expected<void, std::error_code>{plexus::unexpect, std::make_error_code(std::errc::invalid_argument)};
        std::uint32_t v = 0;
        for(int i = 0; i < 4; ++i)
            v |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(bytes[i])) << (8 * i);
        out.value = v;
        return {};
    }

    plexus::type_identity type_info() const
    {
        return {Tag::id, Tag::name, 0};
    }
};

using reading_codec = counter_codec<reading_type>;
using command_codec = counter_codec<command_type>;

static_assert(plexus::typed_codec<reading_codec>);
static_assert(plexus::typed_codec<command_codec>);

inline std::optional<plexus::match::key_pattern> pattern_of(std::string_view topic)
{
    const auto made = plexus::match::key_pattern::make(topic);
    return made.has_value() ? std::optional<plexus::match::key_pattern>{*made} : std::nullopt;
}

// The one type name the peer on the far side declared for a topic, or an empty view if this end
// enumerates no such edge. Both ends ask their OWN node this: the answer is the propagated record,
// which is the whole point of the gate. The role picks the far side's edge out of a topic this end
// also sits on — a subscriber here looks for the publisher there.
template<typename Node>
std::string_view declared_type_of(const Node &node, std::string_view topic, plexus::graph::topic_role role,
                                  std::span<plexus::graph::topic_record> scratch)
{
    const auto swept = node.topics(scratch, pattern_of(topic));
    for(std::size_t i = 0; i < swept.count; ++i)
        if(scratch[i].role == role && scratch[i].types.count == 1)
            return scratch[i].types.names[0];
    return {};
}

}

#endif
