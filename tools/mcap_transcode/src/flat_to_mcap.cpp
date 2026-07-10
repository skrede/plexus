#include "plexus/tools/flat_to_mcap.h"

#include "plexus/io/recording/record_projection.h"

// The compression-disable macros arrive from plexus_mcap_dep; this is the single
// TU that pulls in the mcap implementation (writer for the transcode, reader so a
// linked round-trip consumer needs no second implementation TU). MCAP_IMPLEMENTATION
// must be defined before the first mcap include, so the mcap headers precede the
// emitter header (which includes the writer without the implementation macro).
#define MCAP_IMPLEMENTATION
#include <mcap/writer.hpp>
#include <mcap/reader.hpp>

#include "plexus/tools/detail/mcap_channel_emitter.h"

#include <span>
#include <filesystem>

namespace plexus::tools {

namespace rec = plexus::io::recording;

transcode_result flat_to_mcap(std::span<const std::byte>   flat_stream,
                              const std::filesystem::path &out_mcap)
{
    return flat_to_mcap(flat_stream, out_mcap, schema_provider{}, hint_translator{});
}

transcode_result flat_to_mcap(std::span<const std::byte>   flat_stream,
                              const std::filesystem::path &out_mcap,
                              schema_provider              provider,
                              hint_translator              translate_hint)
{
    transcode_result out;

    auto input = rec::read_projection_input(flat_stream);
    if(!input)
    {
        out.error = "not a plexus flat record-stream (bad header/preamble)";
        return out;
    }
    out.recovered                = input->recovery.recovered;
    out.trailing_partial_dropped = input->recovery.trailing_partial_dropped;
    out.corruption_skipped       = input->recovery.corruption_skipped;

    // Chunked output (the mcap default) builds the Summary section + indexes a viewer needs
    // to avoid an "unindexed file" warning; compression stays None so the chunked writer
    // pulls in no compression-library dependency.
    mcap::McapWriterOptions wopts{"plexus"};
    wopts.compression = mcap::Compression::None;

    mcap::McapWriter writer;
    if(const auto status = writer.open(out_mcap.string(), wopts); !status.ok())
    {
        out.error = "could not open MCAP output: " + status.message;
        return out;
    }

    const auto topic_names     = detail::build_topic_names(input->defs, input->records);
    const auto declared_labels = detail::build_declared_labels(input->defs);
    const auto schema_hints    = detail::build_schema_hints(input->records);
    detail::mcap_emitter emitter{writer, out, topic_names, declared_labels, schema_hints,
                                 provider, translate_hint};
    for(const auto &r : input->records)
        detail::emit_record(emitter, r);

    writer.close();
    out.ok = true;
    return out;
}

}
