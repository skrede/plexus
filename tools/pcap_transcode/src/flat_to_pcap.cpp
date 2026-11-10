#include "plexus/tools/flat_to_pcap.h"

#include "plexus/tools/detail/pcapng_blocks.h"

#include "plexus/io/recording/record_decode.h"
#include "plexus/io/recording/record_format.h"
#include "plexus/io/recording/record_envelope.h"
#include "plexus/io/recording/record_projection.h"

#include "plexus/node_id.h"

#include <span>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <string_view>

namespace plexus::tools {

namespace {

namespace rec = plexus::io::recording;

using namespace detail;

std::string peer_hex(const plexus::node_id &peer)
{
    constexpr std::string_view digits = "0123456789abcdef";
    std::string s;
    s.reserve(peer.size() * 2);
    for(std::byte b : peer)
    {
        const auto v = std::to_integer<std::uint8_t>(b);
        s.push_back(digits[(v >> 4) & 0xf]);
        s.push_back(digits[v & 0xf]);
    }
    return s;
}

std::string_view crypto_token(rec::capture_crypto_position pos)
{
    return pos == rec::capture_crypto_position::ciphertext ? "ciphertext" : "cleartext";
}

std::string_view direction_token(rec::wire_direction dir)
{
    return dir == rec::wire_direction::in ? "in" : "out";
}

// The crypto tap position is fixed for the whole capture, so the Section Header Block carries
// it section-scoped — the authoritative human/C-plugin-readable record. The Lua Dissector API
// cannot reach SHB options at dissect time, so it is ALSO appended to each EPB comment below.
void write_shb(std::vector<std::byte> &out, const rec::stream_definitions &defs)
{
    std::vector<std::byte> body;
    put_u32(body, k_byte_order_magic);
    put_u16(body, 1);                     // major
    put_u16(body, 0);                     // minor
    put_u64(body, 0xFFFFFFFFFFFFFFFFull); // section length unknown
    put_comment(body, std::string{"plexus.crypto_position="}.append(crypto_token(defs.crypto_position)));
    put_comment(body, "plexus.clock_epoch_ns=" + std::to_string(defs.clock_epoch));
    put_endofopt(body);
    seal_block(out, k_shb_type, body);
}

void write_idb(std::vector<std::byte> &out)
{
    std::vector<std::byte> body;
    put_u16(body, k_linktype_user0);
    put_u16(body, 0);           // reserved
    put_u32(body, 0);           // snaplen (no limit)
    const std::byte tsresol{9}; // 10^-9 s (nanoseconds)
    put_option(body, k_opt_if_tsresol, std::span<const std::byte>{&tsresol, 1});
    put_endofopt(body);
    seal_block(out, k_idb_type, body);
}

std::string epb_comment(const rec::decoded_record &r, rec::capture_crypto_position pos)
{
    std::string comment = "seq=" + std::to_string(r.wire_seq);
    comment.append(" dir=").append(direction_token(r.wire_dir));
    comment.append(" peer=").append(peer_hex(r.peer));
    comment.append(" crypto_position=").append(crypto_token(pos));
    return comment;
}

void put_epb_options(std::vector<std::byte> &body, const rec::decoded_record &r, rec::capture_crypto_position pos)
{
    const std::uint32_t flags = r.wire_dir == rec::wire_direction::in ? k_dir_inbound : k_dir_outbound;
    std::vector<std::byte> flag_word;
    put_u32(flag_word, flags);
    put_option(body, k_opt_epb_flags, flag_word);
    put_comment(body, epb_comment(r, pos));
    put_endofopt(body);
}

void write_epb(std::vector<std::byte> &out, const rec::decoded_record &r, rec::capture_crypto_position pos)
{
    std::vector<std::byte> body;
    put_u32(body, 0);                                                        // interface_id
    put_u32(body, static_cast<std::uint32_t>(r.capture_ts >> 32));           // timestamp_high
    put_u32(body, static_cast<std::uint32_t>(r.capture_ts & 0xFFFFFFFFull)); // timestamp_low
    const auto len = static_cast<std::uint32_t>(r.payload.size());
    put_u32(body, len); // captured_len
    put_u32(body, len); // original_len
    put_bytes(body, r.payload);
    pad4(body);
    put_epb_options(body, r, pos);
    seal_block(out, k_epb_type, body);
}

std::size_t write_packets(std::vector<std::byte> &out, const rec::projection_input &input)
{
    std::size_t packets = 0;
    for(const auto &r : input.records)
    {
        if(r.category != rec::record_category::wire_frame)
            continue;
        write_epb(out, r, input.defs.crypto_position);
        ++packets;
    }
    return packets;
}

void save(pcap_result &out, const std::filesystem::path &out_pcapng, std::span<const std::byte> bytes)
{
    std::ofstream file{out_pcapng, std::ios::binary};
    if(!file)
    {
        out.error = "could not open pcapng output: " + out_pcapng.string();
        return;
    }
    file.write(reinterpret_cast<const char *>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    if(!file)
    {
        out.error = "could not write pcapng output: " + out_pcapng.string();
        return;
    }
    out.ok = true;
}

}

pcap_result flat_to_pcap(std::span<const std::byte> flat_stream, const std::filesystem::path &out_pcapng)
{
    pcap_result out;

    auto input = rec::read_projection_input(flat_stream);
    if(!input)
    {
        out.error = "not a plexus flat record-stream (bad header/preamble)";
        return out;
    }
    out.recovered                = input->recovery.recovered;
    out.trailing_partial_dropped = input->recovery.trailing_partial_dropped;
    out.corruption_skipped       = input->recovery.corruption_skipped;

    std::vector<std::byte> bytes;
    write_shb(bytes, input->defs);
    write_idb(bytes);
    out.packets = write_packets(bytes, *input);
    save(out, out_pcapng, bytes);
    return out;
}

}
