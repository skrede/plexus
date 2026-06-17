#ifndef HPP_GUARD_PLEXUS_VALUE_LOGGER_OPTIONS_H
#define HPP_GUARD_PLEXUS_VALUE_LOGGER_OPTIONS_H

#include "plexus/typed_codec.h"

#include "plexus/io/subscriber_qos.h"

#include <ostream>
#include <optional>

namespace plexus {

// The output projection a value_logger writes: a columnar CSV row for plotting, a JSON
// object per line for nested types, or a human text line. csv is the analysis default.
enum class log_format
{
    csv,
    jsonl,
    text
};

// The typed value_logger's construction-time options, the sibling of
// typed_subscriber_options (designated-initializer, required-with-default). The `out`
// stream is the only REQUIRED field (a reference has no default): the consumer names
// where the records go — std::cout, a file stream, any ostream that OUTLIVES the handle.
// The format is required-with-default (csv); posture/type_id/qos mirror the typed
// subscriber. There is deliberately NO recording-QoS capture override: a value_logger is
// a live projecting subscriber, not a recorder declaration.
struct value_logger_options
{
    std::ostream &              out;
    log_format                  format  = log_format::csv;
    io::attach_posture          posture = io::attach_posture::lenient;
    std::optional<type_identity> type_id{};
    io::subscriber_qos          qos{};
};

}

#endif
