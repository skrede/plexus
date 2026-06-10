#ifndef HPP_GUARD_PLEXUS_CRYPTO_DATAGRAM_AUTHENTICATED_CHANNEL_H
#define HPP_GUARD_PLEXUS_CRYPTO_DATAGRAM_AUTHENTICATED_CHANNEL_H

#include "plexus/crypto/anti_replay_window.h"
#include "plexus/crypto/key_schedule.h"
#include "plexus/crypto/aead_cipher.h"

#include "plexus/wire/stream_inbound.h"
#include "plexus/wire/frame.h"

#include "plexus/io/polymorphic_byte_channel.h"
#include "plexus/io/byte_channel.h"
#include "plexus/io/io_error.h"
#include "plexus/detail/compat.h"

#include <span>
#include <array>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <algorithm>

namespace plexus::crypto {

// A datagram byte_channel decorator mirroring dtls_channel's drop-not-teardown
// discipline. Unlike the stream variant (an implicit counter), each datagram carries
// its AEAD sequence EXPLICITLY on the wire so the receiver reconstructs the nonce
// regardless of UDP drop/reorder (a pure implicit counter desyncs on the first
// dropped datagram). An RFC 4303 sliding-window filter rejects replays per epoch.
//
// Wire shape per datagram: seq(8 BE) || key_epoch(1) || frame_header(28) ||
// ciphertext||tag(16). The plaintext frame_header is the AEAD AAD (RFC 8439); only
// the payload is sealed, so the stack above sees plaintext header-on frames.
//
// On a replayed/too-old sequence OR a tag failure the datagram is DROPPED and a
// per-session counter increments — NEVER on_protocol_close (a teardown would hand a
// spoofer a remote-disconnect DoS on ambient UDP garbage), NEVER a per-packet event
// (the observer would become the DoS). The drop counters are occupancy reads,
// readable on demand. This is the AEAD-required datagram path: an auth-only datagram
// configuration is refused at the wiring layer, so this decorator is never installed
// without AEAD.
template <typename Lower>
class datagram_authenticated_channel
{
public:
    datagram_authenticated_channel(Lower &lower, aead_cipher_id cipher,
                                   const derived_keys &keys, std::uint32_t initial_epoch = 0)
        : m_lower(lower)
        , m_cipher(cipher)
        , m_send_key(keys.k_send)
        , m_recv_key(keys.k_recv)
        , m_send_epoch(initial_epoch)
        , m_recv_epoch(initial_epoch)
    {
        wire_lower();
    }

    datagram_authenticated_channel(const datagram_authenticated_channel &) = delete;
    datagram_authenticated_channel &operator=(const datagram_authenticated_channel &) = delete;
    datagram_authenticated_channel(datagram_authenticated_channel &&) = delete;
    datagram_authenticated_channel &operator=(datagram_authenticated_channel &&) = delete;

    void send(std::span<const std::byte> data)
    {
        if(data.size() < wire::header_size)
            return;
        const auto header = data.first(wire::header_size);
        const auto payload = data.subspan(wire::header_size);

        const std::uint64_t seq = m_send_seq++;
        // The nonce epoch field is the 8-bit wire epoch byte (only 8 bits of epoch ride
        // the wire), so seal masks to that domain to match the receiver's open.
        const auto nonce = make_nonce(m_send_epoch & 0xffu, seq);
        if(!seal(m_cipher, m_send_key, nonce, header, payload, m_seal_scratch))
            return;

        m_send_frame.resize(k_seq_len + 1 + wire::header_size + m_seal_scratch.size());
        write_seq(m_send_frame.data(), seq);
        m_send_frame[k_seq_len] = static_cast<std::byte>(m_send_epoch & 0xffu);
        std::copy(header.begin(), header.end(), m_send_frame.begin() + k_seq_len + 1);
        std::copy(m_seal_scratch.begin(), m_seal_scratch.end(),
                  m_send_frame.begin() + k_seq_len + 1 + wire::header_size);
        m_lower.send(m_send_frame);
    }

    void close() { m_lower.close(); }

    [[nodiscard]] io::endpoint remote_endpoint() const { return m_lower.remote_endpoint(); }

    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)> cb) { m_on_data = std::move(cb); }
    void on_closed(plexus::detail::move_only_function<void()> cb) { m_lower.on_closed(std::move(cb)); }
    void on_error(plexus::detail::move_only_function<void(io::io_error)> cb) { m_lower.on_error(std::move(cb)); }
    void on_protocol_close(plexus::detail::move_only_function<void(wire::close_cause)> cb) { m_lower.on_protocol_close(std::move(cb)); }

    [[nodiscard]] std::size_t dropped_count() const noexcept { return m_replay_dropped + m_tamper_dropped; }
    [[nodiscard]] std::size_t replay_count() const noexcept { return m_replay_dropped; }
    [[nodiscard]] std::size_t tamper_dropped_count() const noexcept { return m_tamper_dropped; }
    [[nodiscard]] std::size_t backpressured() const { return m_lower.backpressured(); }

private:
    static constexpr std::size_t k_seq_len = 8;
    static constexpr std::size_t k_frame_overhead = k_seq_len + 1 + wire::header_size + k_aead_tag_len;

    void wire_lower()
    {
        m_lower.on_data([this](std::span<const std::byte> bytes) { on_lower_data(bytes); });
    }

    void on_lower_data(std::span<const std::byte> bytes)
    {
        if(bytes.size() < k_frame_overhead)
        {
            ++m_tamper_dropped;
            return;
        }
        const std::uint64_t seq = read_seq(bytes.data());
        const auto epoch_byte = static_cast<std::uint8_t>(bytes[k_seq_len]);
        const auto header = bytes.subspan(k_seq_len + 1, wire::header_size);
        const auto sealed = bytes.subspan(k_seq_len + 1 + wire::header_size);

        // RFC 4303 §3.4.3 order: verify the tag BEFORE committing any replay/epoch state.
        // The candidate key is resolved without advancing the epoch; a non-mutating probe
        // rejects an obvious replay/too-old without sliding the window; only a successful
        // open commits the epoch advance and marks the sequence seen — so a forged
        // datagram can neither wedge the window nor desync the key.
        aead_key candidate{};
        bool advances_epoch = false;
        if(!candidate_key_for(epoch_byte, candidate, advances_epoch))
        {
            ++m_tamper_dropped;
            return;
        }

        // A next-epoch datagram targets a window that the epoch advance resets, so the
        // replay check only applies within the current epoch. Probe (not commit) so a
        // forged current-epoch sequence cannot slide the window before authentication.
        if(!advances_epoch && m_window.would_accept(seq) != replay_verdict::accept)
        {
            ++m_replay_dropped;
            return;
        }

        const auto nonce = make_nonce(epoch_byte, seq);
        if(!open(m_cipher, candidate, nonce, header, sealed, m_open_scratch))
        {
            ++m_tamper_dropped;
            return;
        }

        if(advances_epoch)
        {
            m_recv_key = candidate;
            ++m_recv_epoch;
            m_window.reset();
        }
        // The probe already accepted; this commits the slide/set now that the tag is
        // verified. The verdict is necessarily accept (no reordering between probe and
        // commit on this single-threaded path), so it is intentionally discarded.
        (void)m_window.check_and_set(seq);

        m_recv_frame.resize(wire::header_size + m_open_scratch.size());
        std::copy(header.begin(), header.end(), m_recv_frame.begin());
        std::copy(m_open_scratch.begin(), m_open_scratch.end(),
                  m_recv_frame.begin() + wire::header_size);
        if(m_on_data)
            m_on_data(std::span<const std::byte>{m_recv_frame});
    }

    // Resolve the recv key that epoch_byte names WITHOUT committing any state: the
    // current epoch returns the current key (advances=false); the next epoch derives the
    // forward key into out (advances=true) but does NOT assign m_recv_key / ++m_recv_epoch
    // / reset the window — the caller commits that only after open() verifies the tag, so
    // a forged next-epoch datagram cannot roll the receiver's key past the sender (RFC
    // 9147 per-epoch reset is gated on authentication). Anything else reports failure.
    bool candidate_key_for(std::uint8_t epoch_byte, aead_key &out, bool &advances) noexcept
    {
        const auto current_low = static_cast<std::uint8_t>(m_recv_epoch & 0xffu);
        if(epoch_byte == current_low)
        {
            out = m_recv_key;
            advances = false;
            return true;
        }
        const auto next_low = static_cast<std::uint8_t>((m_recv_epoch + 1) & 0xffu);
        if(epoch_byte == next_low)
        {
            advances = true;
            return derive_forward(m_recv_key, out);
        }
        return false;
    }

    static bool derive_forward(const aead_key &from, aead_key &out)
    {
        std::array<std::byte, 16> nonce{};
        std::array<std::byte, 32> transcript{};
        derived_keys d{};
        if(!derive_keys(std::span<const std::byte>{from}, nonce, nonce, transcript, d))
            return false;
        out = d.k_send;
        return true;
    }

    static void write_seq(std::byte *p, std::uint64_t seq) noexcept
    {
        for(std::size_t i = 0; i < k_seq_len; ++i)
            p[i] = static_cast<std::byte>((seq >> (8u * (7u - i))) & 0xffu);
    }

    static std::uint64_t read_seq(const std::byte *p) noexcept
    {
        std::uint64_t seq = 0;
        for(std::size_t i = 0; i < k_seq_len; ++i)
            seq = (seq << 8u) | static_cast<std::uint64_t>(static_cast<std::uint8_t>(p[i]));
        return seq;
    }

    // 96-bit RFC 8439 nonce = epoch(4 BE) || sequence(8 BE). The epoch field commits to
    // the 8-bit wire epoch byte on BOTH seal and open (only that byte rides the wire), so
    // the high three epoch bytes are always zero and the two sides agree past epoch 255.
    // Each epoch installs a fresh key, so an 8-bit epoch disambiguator is sufficient for
    // (key,nonce) uniqueness within a key's lifetime; the explicit per-datagram sequence
    // guarantees within-epoch uniqueness.
    static std::array<std::byte, k_aead_nonce_len> make_nonce(std::uint32_t epoch, std::uint64_t seq) noexcept
    {
        std::array<std::byte, k_aead_nonce_len> n{};
        for(std::size_t i = 0; i < 4; ++i)
            n[i] = static_cast<std::byte>((epoch >> (8u * (3u - i))) & 0xffu);
        for(std::size_t i = 0; i < 8; ++i)
            n[4 + i] = static_cast<std::byte>((seq >> (8u * (7u - i))) & 0xffu);
        return n;
    }

    Lower &m_lower;
    aead_cipher_id m_cipher;
    aead_key m_send_key;
    aead_key m_recv_key;
    std::uint32_t m_send_epoch;
    std::uint32_t m_recv_epoch;
    std::uint64_t m_send_seq{0};
    anti_replay_window<> m_window;
    std::size_t m_replay_dropped{0};
    std::size_t m_tamper_dropped{0};
    std::vector<std::byte> m_seal_scratch;
    std::vector<std::byte> m_open_scratch;
    std::vector<std::byte> m_send_frame;
    std::vector<std::byte> m_recv_frame;
    plexus::detail::move_only_function<void(std::span<const std::byte>)> m_on_data;
};

}

static_assert(plexus::io::byte_channel<plexus::crypto::datagram_authenticated_channel<plexus::io::polymorphic_byte_channel>>,
    "datagram_authenticated_channel must satisfy byte_channel — check the seven verbs");

#endif
