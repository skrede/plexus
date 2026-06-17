#ifndef HPP_GUARD_PLEXUS_TOOLS_FLAT_TO_PCAP_H
#define HPP_GUARD_PLEXUS_TOOLS_FLAT_TO_PCAP_H

#include <span>
#include <string>
#include <cstddef>
#include <filesystem>

namespace plexus::tools {

// What the transcode wrote, plus the offline-recovery accounting it read through.
// recovered/trailing_partial_dropped/corruption_skipped come straight from the
// flat-stream reader's recovery scan so a caller can report capture honesty.
struct pcap_result
{
    bool          ok{false};
    std::size_t   packets{0};
    std::size_t   recovered{0};
    bool          trailing_partial_dropped{false};
    bool          corruption_skipped{false};
    std::string   error;
};

// Read a flat plexus capture stream and write a pcapng container to out_pcapng.
//
// Only the wire-frame records are selected; each becomes one Enhanced Packet Block on a
// single DLT_USER0 interface. The framed wire bytes are laid into the packet data verbatim
// (the payload is never decoded or reinterpreted). Direction rides the EPB flags option, the
// nanosecond timestamp comes from the record's capture_ts, and the per-capture crypto tap
// position is carried both section-scoped (in the SHB) and per packet (in each EPB comment).
// flat_stream must stay alive for the duration of the call (the reader borrows it).
pcap_result flat_to_pcap(std::span<const std::byte>   flat_stream,
                         const std::filesystem::path &out_pcapng);

}

#endif
