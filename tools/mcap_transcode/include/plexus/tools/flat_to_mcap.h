#ifndef HPP_GUARD_PLEXUS_TOOLS_FLAT_TO_MCAP_H
#define HPP_GUARD_PLEXUS_TOOLS_FLAT_TO_MCAP_H

#include "plexus/tools/mcap_schema.h"

#include <span>
#include <string>
#include <cstddef>
#include <filesystem>

namespace plexus::tools {

// What the transcode wrote, plus the offline-recovery accounting it read through.
// recovered/trailing_partial_dropped/corruption_skipped come straight from the
// flat-stream reader's recovery scan so a caller can report capture honesty.
struct transcode_result
{
    bool          ok{false};
    std::size_t   schemas{0};
    std::size_t   channels{0};
    std::size_t   messages{0};
    std::size_t   recovered{0};
    bool          trailing_partial_dropped{false};
    bool          corruption_skipped{false};
    std::string   error;
};

// Read a flat plexus capture stream and write an MCAP container to out_mcap.
//
// Sample records become Messages on per-topic Channels (opaque plexus bytes, no schema);
// the control-plane event records ride synthetic per-category channels carrying a
// transcode-synthesized JSON object with a real jsonschema; wire-frame records ride their
// own channel. Sample payload bytes are laid into each Message verbatim — no plexus codec
// runs (serializer-agnostic). flat_stream must stay alive for the duration of the call
// (the reader borrows it).
transcode_result flat_to_mcap(std::span<const std::byte>   flat_stream,
                              const std::filesystem::path &out_mcap);

// The schema-decorating overload. A sample topic's channel takes its schema from, in order:
// the consumer provider keyed on type_id (the escape hatch, wins on conflict); else the
// translator applied to the schema_hint the stream carried for that type_id; else the
// preamble-declared schema; else the opaque schemaId-0 path. Either callable may be empty,
// and two empty callables reproduce the 2-arg behavior exactly. The translator is the only
// seam that reads a hint vocabulary — this transcode names none.
transcode_result flat_to_mcap(std::span<const std::byte>   flat_stream,
                              const std::filesystem::path &out_mcap,
                              schema_provider              provider,
                              hint_translator              translate_hint = {});

}

#endif
