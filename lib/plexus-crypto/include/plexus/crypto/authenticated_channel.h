#ifndef HPP_GUARD_PLEXUS_CRYPTO_AUTHENTICATED_CHANNEL_H
#define HPP_GUARD_PLEXUS_CRYPTO_AUTHENTICATED_CHANNEL_H

#include "plexus/crypto/key_schedule.h"
#include "plexus/crypto/aead_cipher.h"
#include "plexus/crypto/aead_epoch.h"

#include "plexus/wire/stream_inbound.h"
#include "plexus/wire/frame_codec.h"
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

// A stream byte_channel decorator that seals each header-on frame over a wrapped
// lower plaintext channel (the tls_channel discipline — deleted copy/move, the
// seven verbs, on_data re-emitted, tag-fail routed to on_protocol_close). Unlike
// tls_channel (which owns an ssl::stream), this WRAPS a lower channel and
// transforms on send/receive: the stack above still sees plaintext header-on
// frames.
//
// Wire shape per frame: key_epoch(1) || frame_header(28) || ciphertext||tag(16).
// The plaintext frame_header is the AEAD AAD (RFC 8439) so a tampered header is
// caught while staying readable upward; only the payload is encrypted. The nonce
// is the 96-bit epoch||sequence counter (RFC 8439 width) — a per-direction
// monotonic counter, NEVER random and NEVER the ARQ sequence. A bad tag on an
// ordered stream is wire misbehavior with no honest resync, so it routes to
// on_protocol_close (the stream-terminate path), not on_error.
//
// Rekey-before-wrap (RFC 9147): at the rekey threshold the send direction
// re-derives its key from the retiring key and bumps the send epoch; the explicit
// key-epoch byte ahead of the ciphertext names which key sealed a frame. The
// receive direction tracks the peer's send epoch off that byte and derives its
// recv key forward through the identical deterministic chain. On an ordered stream
// a pre-rekey frame cannot arrive after the advance, so the receiver opens only the
// current and the next epoch — there is no previous-epoch drain window (the per-epoch
// sequence resets on advance, so a prior-epoch frame has no counter to open against).
template<typename Lower>
class authenticated_channel
{
public:
    authenticated_channel(Lower &lower, aead_cipher_id cipher, const derived_keys &keys,
                          std::uint32_t initial_epoch   = 0,
                          std::uint64_t rekey_threshold = k_rekey_message_threshold)
            : m_lower(lower)
            , m_cipher(cipher)
            , m_send_key(keys.k_send)
            , m_recv_key(keys.k_recv)
            , m_send_epoch(initial_epoch)
            , m_recv_epoch(initial_epoch)
            , m_rekey_threshold(rekey_threshold)
    {
        wire_lower();
    }

    authenticated_channel(const authenticated_channel &)            = delete;
    authenticated_channel &operator=(const authenticated_channel &) = delete;
    authenticated_channel(authenticated_channel &&)                 = delete;
    authenticated_channel &operator=(authenticated_channel &&)      = delete;

    void send(std::span<const std::byte> data)
    {
        if(data.size() < wire::header_size)
            return;
        const auto header  = data.first(wire::header_size);
        const auto payload = data.subspan(wire::header_size);

        if(m_send_seq >= m_rekey_threshold)
            rekey_send();

        // The nonce epoch field is the 8-bit wire epoch byte (only 8 bits of epoch
        // ride the wire), so seal masks to that domain to match the receiver's open.
        const auto nonce = make_nonce(m_send_epoch & 0xffu, m_send_seq++);
        if(!seal(m_cipher, m_send_key, nonce, header, payload, m_seal_scratch))
            return;

        m_send_frame.resize(1 + wire::header_size + m_seal_scratch.size());
        m_send_frame[0] = static_cast<std::byte>(m_send_epoch & 0xffu);
        std::copy(header.begin(), header.end(), m_send_frame.begin() + 1);
        std::copy(m_seal_scratch.begin(), m_seal_scratch.end(),
                  m_send_frame.begin() + 1 + wire::header_size);
        m_lower.send(m_send_frame);
    }

    void close() { m_lower.close(); }

    [[nodiscard]] io::endpoint remote_endpoint() const { return m_lower.remote_endpoint(); }

    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)> cb)
    {
        m_on_data = std::move(cb);
    }
    void on_closed(plexus::detail::move_only_function<void()> cb)
    {
        m_lower.on_closed(std::move(cb));
    }
    void on_error(plexus::detail::move_only_function<void(io::io_error)> cb)
    {
        m_lower.on_error(std::move(cb));
    }
    void on_protocol_close(plexus::detail::move_only_function<void(wire::close_cause)> cb)
    {
        m_on_protocol_close = std::move(cb);
    }

    [[nodiscard]] std::uint32_t send_epoch() const noexcept { return m_send_epoch; }
    [[nodiscard]] std::uint64_t send_sequence() const noexcept { return m_send_seq; }
    [[nodiscard]] std::size_t   backpressured() const { return m_lower.backpressured(); }

private:
    void wire_lower()
    {
        m_lower.on_data([this](std::span<const std::byte> bytes) { on_lower_data(bytes); });
    }

    void on_lower_data(std::span<const std::byte> bytes)
    {
        if(bytes.size() < 1 + wire::header_size + k_aead_tag_len)
            return handle_protocol_close(wire::close_cause::buffer_overflow);

        const auto epoch_byte = static_cast<std::uint8_t>(bytes[0]);
        const auto header     = bytes.subspan(1, wire::header_size);
        const auto sealed     = bytes.subspan(1 + wire::header_size);

        const aead_key *key = select_recv_key(epoch_byte);
        if(!key)
            return handle_protocol_close(wire::close_cause::invalid_magic);

        const auto nonce = make_nonce(epoch_byte, m_recv_seq);
        if(!open(m_cipher, *key, nonce, header, sealed, m_open_scratch))
            return handle_protocol_close(wire::close_cause::invalid_magic);
        ++m_recv_seq;

        m_recv_frame.resize(wire::header_size + m_open_scratch.size());
        std::copy(header.begin(), header.end(), m_recv_frame.begin());
        std::copy(m_open_scratch.begin(), m_open_scratch.end(),
                  m_recv_frame.begin() + wire::header_size);
        if(m_on_data)
            m_on_data(std::span<const std::byte>{m_recv_frame});
    }

    // The peer advances its send epoch one step at a time; the receiver derives its
    // recv key forward through the identical chain when it first sees the new epoch.
    // A frame naming the current epoch resolves directly; the next epoch advances;
    // anything else is unrecognized.
    const aead_key *select_recv_key(std::uint8_t epoch_byte)
    {
        const auto current_low = static_cast<std::uint8_t>(m_recv_epoch & 0xffu);
        if(epoch_byte == current_low)
            return &m_recv_key;
        const auto next_low = static_cast<std::uint8_t>((m_recv_epoch + 1) & 0xffu);
        if(epoch_byte == next_low)
        {
            aead_key next{};
            if(!derive_forward(m_recv_key, next))
                return nullptr;
            m_recv_key = next;
            ++m_recv_epoch;
            m_recv_seq = 0;
            return &m_recv_key;
        }
        return nullptr;
    }

    void rekey_send()
    {
        aead_key next{};
        if(!derive_forward(m_send_key, next))
            return;
        m_send_key = next;
        ++m_send_epoch;
        m_send_seq = 0;
    }

    void handle_protocol_close(wire::close_cause cause)
    {
        if(m_on_protocol_close)
            m_on_protocol_close(cause);
        close();
    }

    Lower                                                               &m_lower;
    aead_cipher_id                                                       m_cipher;
    aead_key                                                             m_send_key;
    aead_key                                                             m_recv_key;
    std::uint32_t                                                        m_send_epoch;
    std::uint32_t                                                        m_recv_epoch;
    std::uint64_t                                                        m_rekey_threshold;
    std::uint64_t                                                        m_send_seq{0};
    std::uint64_t                                                        m_recv_seq{0};
    std::vector<std::byte>                                               m_seal_scratch;
    std::vector<std::byte>                                               m_open_scratch;
    std::vector<std::byte>                                               m_send_frame;
    std::vector<std::byte>                                               m_recv_frame;
    plexus::detail::move_only_function<void(std::span<const std::byte>)> m_on_data;
    plexus::detail::move_only_function<void(wire::close_cause)>          m_on_protocol_close;
};

}

// The decorator over the erased multi-transport channel is itself a byte_channel:
// the seven-verb conformance is pinned at compile time so the decorated channel
// composes anywhere a plaintext one does.
static_assert(plexus::io::byte_channel<
                      plexus::crypto::authenticated_channel<plexus::io::polymorphic_byte_channel>>,
              "authenticated_channel must satisfy byte_channel — check the seven verbs");

#endif
