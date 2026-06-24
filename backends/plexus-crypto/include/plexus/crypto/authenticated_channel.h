#ifndef HPP_GUARD_PLEXUS_CRYPTO_AUTHENTICATED_CHANNEL_H
#define HPP_GUARD_PLEXUS_CRYPTO_AUTHENTICATED_CHANNEL_H

#include "plexus/crypto/key_schedule.h"
#include "plexus/crypto/aead_cipher.h"
#include "plexus/crypto/aead_epoch.h"
#include "plexus/crypto/detail/aead_path.h"

#include "plexus/wire/close_cause.h"
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
    authenticated_channel(Lower &lower, aead_cipher_id cipher, const derived_keys &keys, std::uint32_t initial_epoch = 0, std::uint64_t rekey_threshold = k_rekey_message_threshold)
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
        detail::stream_aead_send(*this, data);
    }

    void close()
    {
        m_lower.close();
    }

    io::endpoint remote_endpoint() const
    {
        return m_lower.remote_endpoint();
    }

    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)> cb)
    {
        m_on_data_cb = std::move(cb);
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
        m_on_protocol_close_cb = std::move(cb);
    }

    std::uint32_t send_epoch() const noexcept
    {
        return m_send_epoch;
    }
    std::uint64_t send_sequence() const noexcept
    {
        return m_send_seq;
    }
    std::size_t backpressured() const
    {
        return m_lower.backpressured();
    }

private:
    template<typename C>
    friend void detail::rekey_send(C &);
    template<typename C>
    friend void detail::handle_protocol_close(C &, wire::close_cause);
    template<typename C>
    friend void detail::stream_aead_send(C &, std::span<const std::byte>);
    template<typename C>
    friend const aead_key *detail::select_recv_key(C &, std::uint8_t);
    template<typename C>
    friend void detail::stream_aead_on_lower_data(C &, std::span<const std::byte>);

    void wire_lower()
    {
        m_lower.on_data([this](std::span<const std::byte> bytes) { detail::stream_aead_on_lower_data(*this, bytes); });
    }

    Lower &m_lower;
    aead_cipher_id m_cipher;
    aead_key m_send_key;
    aead_key m_recv_key;
    std::uint32_t m_send_epoch;
    std::uint32_t m_recv_epoch;
    std::uint64_t m_rekey_threshold;
    std::uint64_t m_send_seq{0};
    std::uint64_t m_recv_seq{0};
    std::vector<std::byte> m_seal_scratch;
    std::vector<std::byte> m_open_scratch;
    std::vector<std::byte> m_send_frame;
    std::vector<std::byte> m_recv_frame;
    plexus::detail::move_only_function<void(std::span<const std::byte>)> m_on_data_cb;
    plexus::detail::move_only_function<void(wire::close_cause)> m_on_protocol_close_cb;
};

}

// The decorator over the erased multi-transport channel is itself a byte_channel:
// the seven-verb conformance is pinned at compile time so the decorated channel
// composes anywhere a plaintext one does.
static_assert(plexus::io::byte_channel<plexus::crypto::authenticated_channel<plexus::io::polymorphic_byte_channel>>,
              "authenticated_channel must satisfy byte_channel — check the seven verbs");

#endif
