#ifndef HPP_GUARD_PLEXUS_GRAPH_TOPIC_RECORD_H
#define HPP_GUARD_PLEXUS_GRAPH_TOPIC_RECORD_H

#include <cstdint>
#include <optional>
#include <string_view>

namespace plexus::graph
{

struct topic_record
{
    std::string_view name;
    // The producer-declared type id; std::nullopt is the reserved undeclared
    // state, distinct from a zero type id.
    std::optional<std::uint64_t> type;
};

}

#endif
