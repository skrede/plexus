#ifndef HPP_GUARD_PLEXUS_WIRE_DETAIL_HANDSHAKE_CODEC_H
#define HPP_GUARD_PLEXUS_WIRE_DETAIL_HANDSHAKE_CODEC_H

#include "plexus/wire/cursor.h"
#include "plexus/wire/handshake.h"

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace plexus::wire {

// Decode cutoff for the response status byte: the switch enumerates ONLY the defined values; an
// out-of-range byte or a reserved in-range gap matches no case, returns false, and the frame is
// rejected — so no undefined handshake_status enumerator is ever constructed.
inline bool is_defined_handshake_status(std::uint8_t byte) noexcept
{
    switch(static_cast<handshake_status>(byte))
    {
        case handshake_status::accepted:
        case handshake_status::version_incompatible:
        case handshake_status::identity_conflict:
        case handshake_status::rejected_unknown:
        case handshake_status::unauthorized:         return true;
    }
    return false;
}

// Write the shared 87-byte prefix common to both messages (id, versions, fingerprint, the attach
// region, proof). The response encoder appends the trailing status byte after it.
template<typename Msg>
void write_handshake_prefix(writer &w, const Msg &m)
{
    w.bytes(m.id);
    w.u8(m.version_major);
    w.u8(m.version_minor);
    w.u8(m.compatible_version_major);
    w.u8(m.compatible_version_minor);
    w.u8(m.protocol_version);
    w.u64(m.fingerprint);
    w.bytes(m.key_id);
    w.bytes(m.own_nonce);
    w.u8(m.cipher_offer);
    w.u8(m.chosen_cipher);
    w.bytes(m.proof);
}

// Read the shared 87-byte prefix into the message (id, versions, fingerprint, attach region,
// proof). The response decoder reads the trailing status byte after it.
template<typename Msg>
void read_handshake_prefix(reader &r, Msg &m)
{
    r.copy_to(m.id.data(), m.id.size());
    m.version_major            = r.u8();
    m.version_minor            = r.u8();
    m.compatible_version_major = r.u8();
    m.compatible_version_minor = r.u8();
    m.protocol_version         = r.u8();
    m.fingerprint              = r.u64();
    r.copy_to(m.key_id.data(), m.key_id.size());
    r.copy_to(m.own_nonce.data(), m.own_nonce.size());
    m.cipher_offer  = r.u8();
    m.chosen_cipher = r.u8();
    r.copy_to(m.proof.data(), m.proof.size());
}

inline void encode_handshake_request_into(std::vector<std::byte> &out, const handshake_request &req)
{
    out.resize(handshake_request_size);
    writer w{out};
    write_handshake_prefix(w, req);
}

inline void encode_handshake_response_into(std::vector<std::byte>   &out,
                                           const handshake_response &resp)
{
    out.resize(handshake_response_size);
    writer w{out};
    write_handshake_prefix(w, resp);
    w.u8(static_cast<std::uint8_t>(resp.status));
}

inline std::vector<std::byte> encode_handshake_request(const handshake_request &req)
{
    std::vector<std::byte> out;
    encode_handshake_request_into(out, req);
    return out;
}

inline std::vector<std::byte> encode_handshake_response(const handshake_response &resp)
{
    std::vector<std::byte> out;
    encode_handshake_response_into(out, resp);
    return out;
}

inline std::optional<handshake_request> decode_handshake_request(std::span<const std::byte> payload)
{
    if(payload.size() < handshake_request_size)
        return std::nullopt;
    handshake_request req{};
    reader            r{payload};
    read_handshake_prefix(r, req);
    return req;
}

inline std::optional<handshake_response>
decode_handshake_response(std::span<const std::byte> payload)
{
    if(payload.size() < handshake_response_size)
        return std::nullopt;
    handshake_response resp{};
    reader             r{payload};
    read_handshake_prefix(r, resp);
    const auto status_byte = r.u8();
    if(!is_defined_handshake_status(status_byte))
        return std::nullopt;
    resp.status = static_cast<handshake_status>(status_byte);
    return resp;
}

}

#endif
