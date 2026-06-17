#include "plexus/tools/flat_to_pcap.h"

#include "plexus/io/recording/record_decode.h"
#include "plexus/io/recording/record_format.h"
#include "plexus/io/recording/record_envelope.h"
#include "plexus/io/recording/record_projection.h"

#include "plexus/node_id.h"

#include <span>
#include <string>
#include <vector>
#include <cstdint>
#include <fstream>
#include <cstddef>
#include <string_view>

namespace plexus::tools {

namespace {

namespace rec = plexus::io::recording;

// pcapng block + option codes (the on-disk format; every container field little-endian).
constexpr std::uint32_t k_shb_type        = 0x0A0D0D0Au;
constexpr std::uint32_t k_idb_type         = 0x00000001u;
constexpr std::uint32_t k_epb_type         = 0x00000006u;
constexpr std::uint32_t k_byte_order_magic = 0x1A2B3C4Du;
constexpr std::uint16_t k_opt_endofopt     = 0u;
constexpr std::uint16_t k_opt_comment      = 1u;
constexpr std::uint16_t k_opt_if_tsresol   = 9u;
constexpr std::uint16_t k_opt_epb_flags    = 2u;
constexpr std::uint16_t k_linktype_user0   = 147u;  // DLT_USER0

// The pcapng spec maps the epb_flags direction field (low 2 bits) as inbound=1, outbound=2.
constexpr std::uint32_t k_dir_inbound  = 1u;
constexpr std::uint32_t k_dir_outbound = 2u;

void put_u16(std::vector<std::byte> &out, std::uint16_t v)
{
    out.push_back(static_cast<std::byte>(v & 0xff));
    out.push_back(static_cast<std::byte>((v >> 8) & 0xff));
}

void put_u32(std::vector<std::byte> &out, std::uint32_t v)
{
    for(int i = 0; i < 4; ++i)
        out.push_back(static_cast<std::byte>((v >> (8 * i)) & 0xff));
}

void put_u64(std::vector<std::byte> &out, std::uint64_t v)
{
    for(int i = 0; i < 8; ++i)
        out.push_back(static_cast<std::byte>((v >> (8 * i)) & 0xff));
}

void put_bytes(std::vector<std::byte> &out, std::span<const std::byte> bytes)
{
    out.insert(out.end(), bytes.begin(), bytes.end());
}

void pad4(std::vector<std::byte> &out)
{
    while(out.size() % 4 != 0)
        out.push_back(std::byte{0});
}

std::span<const std::byte> as_bytes(std::string_view s)
{
    return std::as_bytes(std::span{s.data(), s.size()});
}

// Append one option [code:u16][length:u16][value padded to 4]. length is the unpadded value size.
void put_option(std::vector<std::byte> &out, std::uint16_t code, std::span<const std::byte> value)
{
    put_u16(out, code);
    put_u16(out, static_cast<std::uint16_t>(value.size()));
    put_bytes(out, value);
    pad4(out);
}

void put_comment(std::vector<std::byte> &out, std::string_view text)
{
    put_option(out, k_opt_comment, as_bytes(text));
}

void put_endofopt(std::vector<std::byte> &out)
{
    put_u16(out, k_opt_endofopt);
    put_u16(out, 0);
}

// Wrap an already-built block body in [type][total_length][body][total_length]: the body is
// the bytes after the leading length, total_length counts the whole block including both
// length words, and the trailing word repeats it so a reader can walk the file backwards.
void seal_block(std::vector<std::byte> &out, std::uint32_t type, std::vector<std::byte> &body)
{
    const auto total = static_cast<std::uint32_t>(body.size() + 12);
    put_u32(out, type);
    put_u32(out, total);
    put_bytes(out, body);
    put_u32(out, total);
}

std::string peer_hex(const plexus::node_id &peer)
{
    static constexpr char digits[] = "0123456789abcdef";
    std::string           s;
    s.reserve(peer.size() * 2);
    for(std::byte b : peer)
    {
        const auto v = static_cast<unsigned>(b);
        s.push_back(digits[(v >> 4) & 0xf]);
        s.push_back(digits[v & 0xf]);
    }
    return s;
}

const char *crypto_token(rec::capture_crypto_position pos)
{
    return pos == rec::capture_crypto_position::ciphertext ? "ciphertext" : "cleartext";
}

// The crypto tap position is fixed for the whole capture, so the Section Header Block carries
// it section-scoped — the authoritative human/C-plugin-readable record. The Lua Dissector API
// cannot reach SHB options at dissect time, so it is ALSO appended to each EPB comment below.
void write_shb(std::vector<std::byte> &out, const rec::stream_definitions &defs)
{
    std::vector<std::byte> body;
    put_u32(body, k_byte_order_magic);
    put_u16(body, 1);  // major
    put_u16(body, 0);  // minor
    put_u64(body, 0xFFFFFFFFFFFFFFFFull);  // section length unknown
    put_comment(body, std::string{"plexus.crypto_position="} + crypto_token(defs.crypto_position));
    put_comment(body, "plexus.clock_epoch_ns=" + std::to_string(defs.clock_epoch));
    put_endofopt(body);
    seal_block(out, k_shb_type, body);
}

void write_idb(std::vector<std::byte> &out)
{
    std::vector<std::byte> body;
    put_u16(body, k_linktype_user0);
    put_u16(body, 0);  // reserved
    put_u32(body, 0);  // snaplen (no limit)
    const std::byte tsresol{9};  // 10^-9 s (nanoseconds)
    put_option(body, k_opt_if_tsresol, std::span<const std::byte>{&tsresol, 1});
    put_endofopt(body);
    seal_block(out, k_idb_type, body);
}

std::string epb_comment(const rec::decoded_record &r, rec::capture_crypto_position pos)
{
    const char *dir = r.wire_dir == rec::wire_direction::in ? "in" : "out";
    return "seq=" + std::to_string(r.wire_seq) + " dir=" + dir + " peer=" + peer_hex(r.peer) +
           " crypto_position=" + crypto_token(pos);
}

void write_epb(std::vector<std::byte> &out, const rec::decoded_record &r,
               rec::capture_crypto_position pos)
{
    std::vector<std::byte> body;
    put_u32(body, 0);  // interface_id
    put_u32(body, static_cast<std::uint32_t>(r.capture_ts >> 32));          // timestamp_high
    put_u32(body, static_cast<std::uint32_t>(r.capture_ts & 0xFFFFFFFFull));  // timestamp_low
    const auto len = static_cast<std::uint32_t>(r.payload.size());
    put_u32(body, len);  // captured_len
    put_u32(body, len);  // original_len
    put_bytes(body, r.payload);
    pad4(body);

    const std::uint32_t flags = r.wire_dir == rec::wire_direction::in ? k_dir_inbound : k_dir_outbound;
    std::vector<std::byte> flag_word;
    put_u32(flag_word, flags);
    put_option(body, k_opt_epb_flags, flag_word);
    put_comment(body, epb_comment(r, pos));
    put_endofopt(body);
    seal_block(out, k_epb_type, body);
}

}

pcap_result flat_to_pcap(std::span<const std::byte>   flat_stream,
                         const std::filesystem::path &out_pcapng)
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
    for(const auto &r : input->records)
    {
        if(r.category != rec::record_category::wire_frame)
            continue;
        write_epb(bytes, r, input->defs.crypto_position);
        ++out.packets;
    }

    std::ofstream file{out_pcapng, std::ios::binary};
    if(!file)
    {
        out.error = "could not open pcapng output: " + out_pcapng.string();
        return out;
    }
    file.write(reinterpret_cast<const char *>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    if(!file)
    {
        out.error = "could not write pcapng output: " + out_pcapng.string();
        return out;
    }
    out.ok = true;
    return out;
}

}
