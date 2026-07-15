// The graph layer's platform-independence gate: it builds the enumeration records, both table
// backends, the declaration codec, and the query sweeps out of core headers alone, so a platform
// #ifdef sneaking into any of them fails this TU. The constrained-target half of the proof is the
// MCU examples compiling the same headers under the xtensa cross-compile; the backend split is a
// template parameter, which is why both tables below fold their edges through one function.
//
// participant_record holds io::endpoint (std::string), which is NOT constexpr-constructible, so the
// static_asserts exercise only the literal parts (the observation enum, the reserved optionals, the
// type-name list); the runtime legs build the records that need storage.

#include "plexus/graph/topic_record.h"
#include "plexus/graph/topic_type_table.h"
#include "plexus/graph/fixed_topic_storage.h"
#include "plexus/graph/participant_record.h"

#include "plexus/node_id.h"

#include "plexus/io/endpoint.h"

#include "plexus/match/key_pattern.h"

#include "plexus/wire/topic_declaration.h"

#include "plexus/detail/topic_sweep.h"

#include <span>
#include <array>
#include <cstdint>
#include <optional>
#include <string_view>

namespace
{

using plexus::graph::observation;
using plexus::graph::participant_record;
using plexus::graph::provenance;
using plexus::graph::route;
using plexus::graph::topic_edge;
using plexus::graph::topic_record;
using plexus::graph::topic_role;
using plexus::graph::type_name_list;
using plexus::graph::upsert_outcome;

constexpr std::uint64_t k_reading_type_id = 0x0B0A0D70ull;

constexpr bool observation_values_distinct()
{
    return observation::directly_observed != observation::reported;
}

constexpr bool direct_only_provenance_has_no_reporter()
{
    const provenance origin{observation::directly_observed, std::nullopt};
    return origin.how == observation::directly_observed && origin.reporter == std::nullopt;
}

// The three declaration states fall out of the name list alone — no second optional beside it: no
// name is undeclared, one empty name is declared-empty, and more than one name IS the polytype
// conflict marker.
constexpr bool type_states_told_apart_by_the_list()
{
    const type_name_list undeclared{{}, 0};
    const type_name_list declared_empty{{std::string_view{}}, 1};
    const type_name_list polytype{{std::string_view{"reading"}, std::string_view{"command"}}, 2};
    return undeclared.count == 0 && declared_empty.names[0].empty() && polytype.count > 1;
}

constexpr bool roles_distinct()
{
    return topic_role::publisher != topic_role::subscriber;
}

static_assert(observation_values_distinct());
static_assert(direct_only_provenance_has_no_reporter());
static_assert(type_states_told_apart_by_the_list());
static_assert(roles_distinct());

plexus::node_id id_with(std::uint8_t tag)
{
    plexus::node_id id{};
    id[0] = std::byte{tag};
    return id;
}

std::optional<plexus::match::key_pattern> pattern_of(std::string_view text)
{
    const auto made = plexus::match::key_pattern::make(text);
    return made.has_value() ? std::optional<plexus::match::key_pattern>{*made} : std::nullopt;
}

// One fold path for both backends: the host default over std::map and the constrained twin over its
// flat array differ by template parameter only. Two roles of one topic are two edges, which is what
// the counts reduce over; the filtered sweep admits the topic its keyset intersects and no other.
template<typename Table>
bool folds_and_answers(Table &table)
{
    const topic_edge produced{id_with(0x01), "telemetry", "reading", k_reading_type_id, topic_role::publisher};
    const topic_edge consumed{id_with(0x02), "telemetry", "reading", k_reading_type_id, topic_role::subscriber};
    if(table.upsert(produced) != upsert_outcome::stored || table.upsert(consumed) != upsert_outcome::stored)
        return false;

    std::array<topic_record, 4> out{};
    const auto matched = plexus::detail::sweep_topics(
            table, std::span<topic_record>{out},
            [filter = pattern_of("telemetry")](const topic_record &rec) { return plexus::detail::topic_in(rec.name, filter); });
    const auto missed = plexus::detail::sweep_topics(
            table, std::span<topic_record>{out},
            [filter = pattern_of("command")](const topic_record &rec) { return plexus::detail::topic_in(rec.name, filter); });

    return matched.count == 2 && !matched.truncated && missed.count == 0
           && out[0].types.count == 1 && out[0].types.names[0] == "reading"
           && plexus::detail::count_topic_role(table, "telemetry", topic_role::publisher) == 1
           && plexus::detail::count_topic_role(table, "telemetry", topic_role::subscriber) == 1
           && table.dropped() == 0 && table.truncations() == 0;
}

bool declaration_round_trips()
{
    const plexus::wire::topic_declaration declared{
            0xA1A2A3A4ull, k_reading_type_id, "telemetry", "reading", plexus::wire::type_state::declared};
    const auto encoded = plexus::wire::encode_topic_declaration(declared);
    const auto decoded = plexus::wire::decode_topic_declaration(encoded);
    return decoded.has_value() && *decoded == declared;
}

bool participant_reads_back_direct_only()
{
    const participant_record rec{
            id_with(0x01),
            route{plexus::io::endpoint{"udp", "203.0.113.7:9000"}, std::nullopt},
            provenance{observation::directly_observed, std::nullopt}};
    return rec.reach.via == std::nullopt && rec.origin.how == observation::directly_observed
           && rec.origin.reporter == std::nullopt;
}

}

int main()
{
    plexus::graph::topic_type_table host_table;
    plexus::graph::basic_topic_type_table<plexus::graph::fixed_topic_storage<4, 2>> device_table;

    const bool tables = folds_and_answers(host_table) && folds_and_answers(device_table);
    return (participant_reads_back_direct_only() && tables && declaration_round_trips()) ? 0 : 1;
}
