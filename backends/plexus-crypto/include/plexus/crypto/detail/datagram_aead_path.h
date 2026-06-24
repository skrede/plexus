#ifndef HPP_GUARD_PLEXUS_CRYPTO_DETAIL_DATAGRAM_AEAD_PATH_H
#define HPP_GUARD_PLEXUS_CRYPTO_DETAIL_DATAGRAM_AEAD_PATH_H

#include "plexus/crypto/anti_replay_window.h"
#include "plexus/crypto/key_schedule.h"
#include "plexus/crypto/aead_cipher.h"
#include "plexus/crypto/aead_epoch.h"

#include "plexus/wire/frame.h"

#include "plexus/io/locality.h"
#include "plexus/io/detail/drop_event.h"

#include <span>
#include <cstddef>
#include <cstdint>
#include <algorithm>

namespace plexus::crypto::detail {

// The per-datagram seal/open AEAD path for datagram_authenticated_channel, relocated by
// friendship. RELOCATION ONLY — the seal/open calls, the nonce construction, the epoch/key
// handling, and the RFC 4303/9147 verify-before-commit order are byte-identical to the channel
// bodies they replace.

template<typename Ch>
void emit_drop(Ch &c, io::detail::drop_cause cause, io::locality transport = io::locality::remote)
{
    if(c.m_on_drop)
        c.m_on_drop(io::detail::drop_event{.cause = cause, .transport = transport});
}

template<typename Ch>
void datagram_send(Ch &c, std::span<const std::byte> data)
{
    if(data.size() < wire::header_size)
    {
        // A frame too small to carry the AEAD-AAD header is a local malformed input, surfaced
        // through the drop seam (the locally-sourced tier) so the caller observes the dropped send.
        emit_drop(c, io::detail::drop_cause::malformed, io::locality::local);
        return;
    }
    const auto header  = data.first(wire::header_size);
    const auto payload = data.subspan(wire::header_size);

    const std::uint64_t seq = c.m_send_seq++;
    // Only 8 bits of epoch ride the wire, so seal masks to that domain to match the receiver's
    // open.
    const auto nonce = make_nonce(c.m_send_epoch & 0xffu, seq);
    if(!seal(c.m_cipher, c.m_send_key, nonce, header, payload, c.m_seal_scratch))
        return;

    c.m_send_frame.resize(Ch::k_seq_len + 1 + wire::header_size + c.m_seal_scratch.size());
    Ch::write_seq(c.m_send_frame.data(), seq);
    c.m_send_frame[Ch::k_seq_len] = static_cast<std::byte>(c.m_send_epoch & 0xffu);
    std::copy(header.begin(), header.end(), c.m_send_frame.begin() + Ch::k_seq_len + 1);
    std::copy(c.m_seal_scratch.begin(), c.m_seal_scratch.end(), c.m_send_frame.begin() + Ch::k_seq_len + 1 + wire::header_size);
    c.m_lower.send(c.m_send_frame);
}

// Resolve the recv key epoch_byte names WITHOUT committing state: the current epoch returns the
// current key (advances=false); the next epoch derives the forward key into out (advances=true)
// but does NOT assign m_recv_key/++m_recv_epoch/reset the window — the caller commits that only
// after open() verifies the tag (RFC 9147 per-epoch reset gated on authentication).
template<typename Ch>
bool candidate_key_for(Ch &c, std::uint8_t epoch_byte, aead_key &out, bool &advances) noexcept
{
    const auto current_low = static_cast<std::uint8_t>(c.m_recv_epoch & 0xffu);
    if(epoch_byte == current_low)
    {
        out      = c.m_recv_key;
        advances = false;
        return true;
    }
    const auto next_low = static_cast<std::uint8_t>((c.m_recv_epoch + 1) & 0xffu);
    if(epoch_byte == next_low)
    {
        // BOUNDED WORK: one forward-KDF per next-epoch datagram before the tag check (the replay
        // window cannot gate it — the advance resets that window). Exactly one HKDF per inbound
        // datagram, bounded by the link rate with no amplification.
        advances = true;
        return derive_forward(c.m_recv_key, out);
    }
    return false;
}

template<typename Ch>
// NOLINTNEXTLINE(readability-function-size)
void datagram_on_lower_data(Ch &c, std::span<const std::byte> bytes)
{
    if(bytes.size() < Ch::k_frame_overhead)
    {
        ++c.m_tamper_dropped;
        emit_drop(c, io::detail::drop_cause::tamper);
        return;
    }
    const std::uint64_t seq        = Ch::read_seq(bytes.data());
    const auto          epoch_byte = static_cast<std::uint8_t>(bytes[Ch::k_seq_len]);
    const auto          header     = bytes.subspan(Ch::k_seq_len + 1, wire::header_size);
    const auto          sealed     = bytes.subspan(Ch::k_seq_len + 1 + wire::header_size);

    // RFC 4303 §3.4.3 order: verify the tag BEFORE committing any replay/epoch state. The
    // candidate key is resolved without advancing the epoch; only a successful open commits the
    // epoch advance and marks the sequence seen.
    aead_key candidate{};
    bool     advances_epoch = false;
    if(!candidate_key_for(c, epoch_byte, candidate, advances_epoch))
    {
        ++c.m_tamper_dropped;
        emit_drop(c, io::detail::drop_cause::tamper);
        return;
    }

    // A next-epoch datagram targets a window the advance resets, so the replay check only applies
    // within the current epoch. Probe (not commit) so a forged current-epoch sequence cannot slide
    // the window before authentication.
    if(!advances_epoch)
    {
        const auto verdict = c.m_window.would_accept(seq);
        if(verdict != replay_verdict::accept)
        {
            ++c.m_replay_dropped;
            emit_drop(c, verdict == replay_verdict::reject_old ? io::detail::drop_cause::too_old : io::detail::drop_cause::replay);
            return;
        }
    }

    const auto nonce = make_nonce(epoch_byte, seq);
    if(!open(c.m_cipher, candidate, nonce, header, sealed, c.m_open_scratch))
    {
        ++c.m_tamper_dropped;
        emit_drop(c, io::detail::drop_cause::tamper);
        return;
    }

    if(advances_epoch)
    {
        c.m_recv_key = candidate;
        ++c.m_recv_epoch;
        c.m_window.reset();
    }
    // The probe already accepted; this commits the slide/set now that the tag is verified. The
    // verdict is necessarily accept on this single-threaded path, so it is intentionally discarded.
    (void)c.m_window.check_and_set(seq);

    c.m_recv_frame.resize(wire::header_size + c.m_open_scratch.size());
    std::copy(header.begin(), header.end(), c.m_recv_frame.begin());
    std::copy(c.m_open_scratch.begin(), c.m_open_scratch.end(), c.m_recv_frame.begin() + wire::header_size);
    if(c.m_on_data)
        c.m_on_data(std::span<const std::byte>{c.m_recv_frame});
}

}

#endif
