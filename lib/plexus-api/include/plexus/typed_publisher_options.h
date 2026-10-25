#ifndef HPP_GUARD_PLEXUS_TYPED_PUBLISHER_OPTIONS_H
#define HPP_GUARD_PLEXUS_TYPED_PUBLISHER_OPTIONS_H

#include "plexus/typed_codec.h"
#include "plexus/topic_qos.h"
#include "plexus/recording_qos.h"

#include "plexus/io/same_host_geometry.h"

#include <cstddef>
#include <optional>

namespace plexus {

// type_id overrides the identity of a codec that carries one; absent, the codec's own is declared. capture and
// geometry are optional because absence is meaningful: unset falls back to the node-level (capture) or
// transport-level (geometry) default, present overrides per topic.
struct typed_publisher_options
{
    topic_qos qos{};
    bool emit_source_identity = false;
    std::size_t pool_depth    = 8;
    std::optional<type_identity> type_id{};
    std::optional<io::same_host_geometry> geometry{};
    std::optional<recording_qos> capture{};
};

}

#endif
