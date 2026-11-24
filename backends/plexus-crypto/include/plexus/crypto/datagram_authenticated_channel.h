#ifndef HPP_GUARD_PLEXUS_CRYPTO_DATAGRAM_AUTHENTICATED_CHANNEL_H
#define HPP_GUARD_PLEXUS_CRYPTO_DATAGRAM_AUTHENTICATED_CHANNEL_H

#include "plexus/crypto/aead_epoch.h"
#include "plexus/crypto/aead_cipher.h"
#include "plexus/crypto/key_schedule.h"

#include "plexus/crypto/detail/aead_path.h"
#include "plexus/crypto/detail/datagram_aead_path.h"

#include "plexus/datagram/anti_replay_window.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/close_cause.h"

#include "plexus/io/io_error.h"
#include "plexus/io/byte_channel.h"
#include "plexus/io/polymorphic_byte_channel.h"

#include "plexus/io/detail/drop_event.h"

#include "plexus/detail/compat.h"

#include <span>
#include <array>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <algorithm>

namespace plexus::crypto {

// A datagram byte_channel decorator with dtls_channel's drop-not-teardown discipline. Each datagram carries its AEAD
// sequence explicitly, so the receiver rebuilds the nonce across UDP drop and reorder, and an RFC 4303 sliding window
// rejects replays per epoch. Wire shape: seq(8 BE) || key_epoch(1) || frame_header(28) || ciphertext||tag(16), the
// plaintext header being the AEAD AAD (RFC 8439). A replayed, too-old or tag-failing datagram is dropped and counted,
// never closed: a teardown would hand a spoofer a remote-disconnect DoS on ambient UDP garbage.
template<typename Lower>
class datagram_authenticated_channel
{
public:
    datagram_authenticated_channel(Lower &lower, aead_cipher_id cipher, const derived_keys &keys, std::uint32_t initial_epoch = 0,
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

    datagram_authenticated_channel(const datagram_authenticated_channel &)            = delete;
    datagram_authenticated_channel &operator=(const datagram_authenticated_channel &) = delete;
    datagram_authenticated_channel(datagram_authenticated_channel &&)                 = delete;
    datagram_authenticated_channel &operator=(datagram_authenticated_channel &&)      = delete;

    void send(std::span<const std::byte> bytes)
    {
        detail::datagram_send(*this, bytes);
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
        m_lower.on_protocol_close(std::move(cb));
    }

    // The sink posts, so a per-packet drop never runs the observer synchronously; the observer would become the DoS.
    void on_drop(plexus::detail::move_only_function<void(const io::detail::drop_event &)> cb)
    {
        m_on_drop_cb = std::move(cb);
    }

    std::size_t dropped_count() const noexcept
    {
        return m_replay_dropped + m_tamper_dropped;
    }

    std::size_t replay_count() const noexcept
    {
        return m_replay_dropped;
    }

    std::size_t tamper_dropped_count() const noexcept
    {
        return m_tamper_dropped;
    }

    std::size_t backpressured() const
    {
        return m_lower.backpressured();
    }

    std::uint64_t scheduler_key() const
        requires requires(const Lower &l) { l.scheduler_key(); }
    {
        return m_lower.scheduler_key();
    }

private:
    template<typename C>
    friend void detail::rekey_send(C &);
    template<typename C>
    friend void detail::emit_drop(C &, io::detail::drop_cause, io::locality);
    template<typename C>
    friend void detail::write_sealed_frame(C &, std::uint64_t, std::span<const std::byte>);
    template<typename C>
    friend void detail::datagram_send(C &, std::span<const std::byte>);
    template<typename C>
    friend bool detail::candidate_key_for(C &, std::uint8_t, aead_key &, bool &) noexcept;
    template<typename C>
    friend void detail::datagram_on_lower_data(C &, std::span<const std::byte>);

    static constexpr std::size_t k_seq_len        = 8;
    static constexpr std::size_t k_frame_overhead = k_seq_len + 1 + wire::header_size + k_aead_tag_len;

    void wire_lower()
    {
        m_lower.on_data([this](std::span<const std::byte> bytes) { detail::datagram_on_lower_data(*this, bytes); });
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

    Lower &m_lower;
    aead_cipher_id m_cipher;
    aead_key m_send_key;
    aead_key m_recv_key;
    std::uint32_t m_send_epoch;
    std::uint32_t m_recv_epoch;
    std::uint64_t m_rekey_threshold;
    std::uint64_t m_send_seq{0};
    datagram::anti_replay_window<> m_window;
    std::size_t m_replay_dropped{0};
    std::size_t m_tamper_dropped{0};
    std::vector<std::byte> m_seal_scratch;
    std::vector<std::byte> m_open_scratch;
    std::vector<std::byte> m_send_frame;
    std::vector<std::byte> m_recv_frame;
    plexus::detail::move_only_function<void(std::span<const std::byte>)> m_on_data_cb;
    plexus::detail::move_only_function<void(const io::detail::drop_event &)> m_on_drop_cb;
};

}

static_assert(plexus::io::byte_channel<plexus::crypto::datagram_authenticated_channel<plexus::io::polymorphic_byte_channel>>,
              "datagram_authenticated_channel must satisfy byte_channel — check the seven verbs");

#endif
