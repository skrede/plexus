#ifndef HPP_GUARD_PLEXUS_TYPED_PUBLISHER_OPTIONS_H
#define HPP_GUARD_PLEXUS_TYPED_PUBLISHER_OPTIONS_H

#include "plexus/typed_codec.h"
#include "plexus/topic_qos.h"
#include "plexus/recording_qos.h"

#include <cstddef>
#include <optional>

namespace plexus {

// An explicit type_id overrides the codec's own. capture is optional because absence is meaningful:
// unset falls back to the node-level default, present overrides per topic. geometry is an OPAQUE
// per-topic same-host provisioning override (null = the node-level default): a producer-side local
// value the backend front-door fills with its concrete geometry type; the generic api stays
// transport-name-free.
struct typed_publisher_options
{
    topic_qos qos{};
    bool emit_source_identity = false;
    std::size_t pool_depth    = 8;
    std::optional<type_identity> type_id{};
    const void *geometry = nullptr;
    std::optional<recording_qos> capture{};
};

}

#endif
