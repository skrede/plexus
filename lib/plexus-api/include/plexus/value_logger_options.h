#ifndef HPP_GUARD_PLEXUS_VALUE_LOGGER_OPTIONS_H
#define HPP_GUARD_PLEXUS_VALUE_LOGGER_OPTIONS_H

#include "plexus/typed_codec.h"

#include "plexus/io/subscriber_qos.h"

#include <ostream>
#include <optional>

namespace plexus {

enum class log_format
{
    csv,
    jsonl,
    text
};

// out is the only required field (a reference has no default) and must outlive the handle.
struct value_logger_options
{
    std::ostream &out;
    log_format format          = log_format::csv;
    io::attach_posture posture = io::attach_posture::lenient;
    std::optional<type_identity> type_id{};
    io::subscriber_qos qos{};
};

}

#endif
