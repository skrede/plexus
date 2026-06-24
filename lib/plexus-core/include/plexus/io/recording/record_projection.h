#ifndef HPP_GUARD_PLEXUS_IO_RECORDING_RECORD_PROJECTION_H
#define HPP_GUARD_PLEXUS_IO_RECORDING_RECORD_PROJECTION_H

#include "plexus/io/recording/record_decode.h"
#include "plexus/io/recording/record_stream_reader.h"

#include <span>
#include <deque>
#include <vector>
#include <cstddef>
#include <optional>

namespace plexus::io::recording {

// Each decoded_record::payload is repointed at a buffer in `payloads`, so the set does not alias
// the input stream. The buffers live in a std::deque so a record's span stays valid as more are
// added (deque elements are node-stable, unlike a reallocating std::vector).
struct projection_input
{
    stream_definitions defs;
    std::vector<decoded_record> records;
    recovery_result recovery;
    std::deque<std::vector<std::byte>> payloads;
};

// Returns nullopt iff the stream is not a plexus flat record-stream (a failed read_definitions).
// The returned set deep-copies its payloads, so `stream` need not outlive the call.
inline std::optional<projection_input> read_projection_input(std::span<const std::byte> stream)
{
    projection_input in;
    record_stream_reader reader{stream};
    if(!reader.read_definitions(in.defs))
        return std::nullopt;
    in.recovery = reader.recover(in.records);

    for(decoded_record &rec : in.records)
        in.payloads.emplace_back(rec.payload.begin(), rec.payload.end());

    auto buf = in.payloads.begin();
    for(decoded_record &rec : in.records)
        rec.payload = std::span<const std::byte>{*buf++};

    return in;
}

}

#endif
