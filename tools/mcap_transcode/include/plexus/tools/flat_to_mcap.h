#ifndef HPP_GUARD_PLEXUS_TOOLS_FLAT_TO_MCAP_H
#define HPP_GUARD_PLEXUS_TOOLS_FLAT_TO_MCAP_H

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
// Sample records become Messages on per-topic Channels; the control-plane event
// records ride synthetic per-category channels; wire-frame records ride their own
// channel. The raw payload bytes are laid into each Message verbatim and the
// encoding is named in the Schema — no plexus codec runs (serializer-agnostic).
// flat_stream must stay alive for the duration of the call (the reader borrows it).
transcode_result flat_to_mcap(std::span<const std::byte>   flat_stream,
                              const std::filesystem::path &out_mcap);

}

#endif
