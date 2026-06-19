#ifndef HPP_GUARD_PLEXUS_CRYPTO_DETAIL_AEAD_PATH_H
#define HPP_GUARD_PLEXUS_CRYPTO_DETAIL_AEAD_PATH_H

#include "plexus/crypto/key_schedule.h"
#include "plexus/crypto/aead_cipher.h"
#include "plexus/crypto/aead_epoch.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/stream_inbound.h"

#include <span>
#include <cstddef>
#include <cstdint>
#include <algorithm>

namespace plexus::crypto::detail {

// The per-frame seal/open AEAD path for the stream authenticated_channel, relocated by
// friendship. RELOCATION ONLY — the seal/open calls, the nonce construction, the rekey-forward
// epoch/key chain (RFC 9147), and the tag-fail-to-protocol-close routing are byte-identical to the
// channel bodies they replace.

template<typename Ch>
void rekey_send(Ch &c)
{
    aead_key next{};
    if(!derive_forward(c.m_send_key, next))
        return;
    c.m_send_key = next;
    ++c.m_send_epoch;
    c.m_send_seq = 0;
}

template<typename Ch>
void handle_protocol_close(Ch &c, wire::close_cause cause)
{
    if(c.m_on_protocol_close)
        c.m_on_protocol_close(cause);
    c.close();
}

template<typename Ch>
void stream_aead_send(Ch &c, std::span<const std::byte> data)
{
    if(data.size() < wire::header_size)
        return;
    const auto header  = data.first(wire::header_size);
    const auto payload = data.subspan(wire::header_size);

    if(c.m_send_seq >= c.m_rekey_threshold)
        rekey_send(c);

    // Only 8 bits of epoch ride the wire, so seal masks to that domain to match the receiver's
    // open.
    const auto nonce = make_nonce(c.m_send_epoch & 0xffu, c.m_send_seq++);
    if(!seal(c.m_cipher, c.m_send_key, nonce, header, payload, c.m_seal_scratch))
        return;

    c.m_send_frame.resize(1 + wire::header_size + c.m_seal_scratch.size());
    c.m_send_frame[0] = static_cast<std::byte>(c.m_send_epoch & 0xffu);
    std::copy(header.begin(), header.end(), c.m_send_frame.begin() + 1);
    std::copy(c.m_seal_scratch.begin(), c.m_seal_scratch.end(),
              c.m_send_frame.begin() + 1 + wire::header_size);
    c.m_lower.send(c.m_send_frame);
}

// The peer advances its send epoch one step at a time; the receiver derives its recv key forward
// through the identical chain when it first sees the new epoch. The current epoch resolves
// directly; the next epoch advances; anything else is unrecognized.
template<typename Ch>
const aead_key *select_recv_key(Ch &c, std::uint8_t epoch_byte)
{
    const auto current_low = static_cast<std::uint8_t>(c.m_recv_epoch & 0xffu);
    if(epoch_byte == current_low)
        return &c.m_recv_key;
    const auto next_low = static_cast<std::uint8_t>((c.m_recv_epoch + 1) & 0xffu);
    if(epoch_byte == next_low)
    {
        aead_key next{};
        if(!derive_forward(c.m_recv_key, next))
            return nullptr;
        c.m_recv_key = next;
        ++c.m_recv_epoch;
        c.m_recv_seq = 0;
        return &c.m_recv_key;
    }
    return nullptr;
}

template<typename Ch>
void stream_aead_on_lower_data(Ch &c, std::span<const std::byte> bytes)
{
    if(bytes.size() < 1 + wire::header_size + k_aead_tag_len)
        return handle_protocol_close(c, wire::close_cause::buffer_overflow);

    const auto epoch_byte = static_cast<std::uint8_t>(bytes[0]);
    const auto header     = bytes.subspan(1, wire::header_size);
    const auto sealed     = bytes.subspan(1 + wire::header_size);

    const aead_key *key = select_recv_key(c, epoch_byte);
    if(!key)
        return handle_protocol_close(c, wire::close_cause::invalid_magic);

    const auto nonce = make_nonce(epoch_byte, c.m_recv_seq);
    if(!open(c.m_cipher, *key, nonce, header, sealed, c.m_open_scratch))
        return handle_protocol_close(c, wire::close_cause::invalid_magic);
    ++c.m_recv_seq;

    c.m_recv_frame.resize(wire::header_size + c.m_open_scratch.size());
    std::copy(header.begin(), header.end(), c.m_recv_frame.begin());
    std::copy(c.m_open_scratch.begin(), c.m_open_scratch.end(),
              c.m_recv_frame.begin() + wire::header_size);
    if(c.m_on_data)
        c.m_on_data(std::span<const std::byte>{c.m_recv_frame});
}

}

#endif
