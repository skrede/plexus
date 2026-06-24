#ifndef HPP_GUARD_PLEXUS_CRYPTO_DATAGRAM_AUTHENTICATED_CHANNEL_H
#define HPP_GUARD_PLEXUS_CRYPTO_DATAGRAM_AUTHENTICATED_CHANNEL_H

#include "plexus/crypto/anti_replay_window.h"
#include "plexus/crypto/key_schedule.h"
#include "plexus/crypto/aead_cipher.h"
#include "plexus/crypto/aead_epoch.h"
#include "plexus/crypto/detail/datagram_aead_path.h"

#include "plexus/wire/close_cause.h"
#include "plexus/wire/frame.h"

#include "plexus/io/detail/drop_event.h"
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
template<typename Lower>
class datagram_authenticated_channel
{
public:
    datagram_authenticated_channel(Lower &lower, aead_cipher_id cipher, const derived_keys &keys, std::uint32_t initial_epoch = 0)
            : m_lower(lower)
            , m_cipher(cipher)
            , m_send_key(keys.k_send)
            , m_recv_key(keys.k_recv)
            , m_send_epoch(initial_epoch)
            , m_recv_epoch(initial_epoch)
    {
        wire_lower();
    }

    datagram_authenticated_channel(const datagram_authenticated_channel &)            = delete;
    datagram_authenticated_channel &operator=(const datagram_authenticated_channel &) = delete;
    datagram_authenticated_channel(datagram_authenticated_channel &&)                 = delete;
    datagram_authenticated_channel &operator=(datagram_authenticated_channel &&)      = delete;

    void send(std::span<const std::byte> data)
    {
        detail::datagram_send(*this, data);
    }

    void close()
    {
        m_lower.close();
    }

    [[nodiscard]] io::endpoint remote_endpoint() const
    {
        return m_lower.remote_endpoint();
    }

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
        m_lower.on_protocol_close(std::move(cb));
    }

    // The drop-observability seam (null by default — zero cost when unobserved). The
    // owner installs the engine's posted drop_sink; this decorator increments its
    // occupancy counter at each drop site AND hands a coalesced event to the sink. The
    // sink POSTS, so the per-packet site never fires the observer synchronously (the DoS
    // guard). peer/topic are left default here — this decorator sits below the demux that
    // knows them; the cause is the load-bearing field.
    void on_drop(plexus::detail::move_only_function<void(const io::detail::drop_event &)> cb)
    {
        m_on_drop = std::move(cb);
    }

    [[nodiscard]] std::size_t dropped_count() const noexcept
    {
        return m_replay_dropped + m_tamper_dropped;
    }
    [[nodiscard]] std::size_t replay_count() const noexcept
    {
        return m_replay_dropped;
    }
    [[nodiscard]] std::size_t tamper_dropped_count() const noexcept
    {
        return m_tamper_dropped;
    }
    [[nodiscard]] std::size_t backpressured() const
    {
        return m_lower.backpressured();
    }

private:
    template<typename C>
    friend void detail::emit_drop(C &, io::detail::drop_cause, io::locality);
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

    Lower                                                                   &m_lower;
    aead_cipher_id                                                           m_cipher;
    aead_key                                                                 m_send_key;
    aead_key                                                                 m_recv_key;
    std::uint32_t                                                            m_send_epoch;
    std::uint32_t                                                            m_recv_epoch;
    std::uint64_t                                                            m_send_seq{0};
    anti_replay_window<>                                                     m_window;
    std::size_t                                                              m_replay_dropped{0};
    std::size_t                                                              m_tamper_dropped{0};
    std::vector<std::byte>                                                   m_seal_scratch;
    std::vector<std::byte>                                                   m_open_scratch;
    std::vector<std::byte>                                                   m_send_frame;
    std::vector<std::byte>                                                   m_recv_frame;
    plexus::detail::move_only_function<void(std::span<const std::byte>)>     m_on_data;
    plexus::detail::move_only_function<void(const io::detail::drop_event &)> m_on_drop;
};

}

static_assert(plexus::io::byte_channel<plexus::crypto::datagram_authenticated_channel<plexus::io::polymorphic_byte_channel>>,
              "datagram_authenticated_channel must satisfy byte_channel — check the seven verbs");

#endif
