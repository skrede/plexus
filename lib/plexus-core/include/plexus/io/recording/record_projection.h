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

// The decoded output every host projector shares: the preamble, the recovered record
// set, and the recovery accounting. It OWNS its payloads — each decoded_record::payload
// is repointed at a buffer held in `payloads`, so the set does not alias the input stream
// and a projector can outlive (or overwrite) that stream without a use-after-free. The
// payload buffers live in a std::deque so a record's span stays valid as more are added
// (deque elements are node-stable, unlike a reallocating std::vector). Treat `payloads`
// as opaque backing storage; read the bytes through each record's payload span.
struct projection_input
{
    stream_definitions                 defs;
    std::vector<decoded_record>        records;
    recovery_result                    recovery;
    std::deque<std::vector<std::byte>> payloads;
};

// Read the header + preamble and recover the data section in one call. Returns nullopt
// iff the stream is not a plexus flat record-stream (a failed read_definitions). The
// returned set owns its payloads (deep-copied here), so `stream` need not outlive the
// call — see projection_input's lifetime contract. The alloc per record is intentional:
// this is an offline host seam, never the producer hot path.
inline std::optional<projection_input> read_projection_input(std::span<const std::byte> stream)
{
    projection_input     in;
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
