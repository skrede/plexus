#ifndef HPP_GUARD_PLEXUS_TESTS_CRYPTO_TEST_NONCE_UNIQUENESS_COMMON_H
#define HPP_GUARD_PLEXUS_TESTS_CRYPTO_TEST_NONCE_UNIQUENESS_COMMON_H

// The named nonce-uniqueness gate (looped, reproducible). Across normal steady-state
// flow, a reconnect (a fresh session minting new keys + a fresh epoch axis), and a
// forced session restart, NO (key-epoch, nonce, direction) tuple ever repeats. The
// AEAD nonce is the deterministic per-direction epoch||sequence counter — never a
// CSPRNG draw, and never the ARQ sequence (a retransmit-reused ARQ sequence does not
// reuse an AEAD nonce).

#include "plexus/crypto/authenticated_channel.h"
#include "plexus/crypto/key_schedule.h"
#include "plexus/crypto/aead_cipher.h"

#include "plexus/io/byte_channel.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/frame.h"

#include <catch2/catch_test_macros.hpp>

#include <set>
#include <span>
#include <tuple>
#include <array>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <functional>

namespace nonce_uniqueness_fixture {

using plexus::crypto::aead_cipher_id;
using plexus::crypto::authenticated_channel;
using plexus::crypto::derived_keys;
using plexus::crypto::derive_keys;

class wire_lower
{
public:
    void send(std::span<const std::byte> data)
    {
        m_last.assign(data.begin(), data.end());
        if(m_sink)
            m_sink(std::span<const std::byte>{m_last});
    }
    void                               close() {}
    [[nodiscard]] plexus::io::endpoint remote_endpoint() const { return {"wire", ""}; }
    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)> cb)
    {
        m_on_data = std::move(cb);
    }
    void on_closed(plexus::detail::move_only_function<void()> cb) { m_on_closed = std::move(cb); }
    void on_error(plexus::detail::move_only_function<void(plexus::io::io_error)> cb)
    {
        m_on_error = std::move(cb);
    }
    void on_protocol_close(plexus::detail::move_only_function<void(plexus::wire::close_cause)> cb)
    {
        m_on_protocol_close = std::move(cb);
    }
    [[nodiscard]] std::size_t backpressured() const { return 0; }

    std::function<void(std::span<const std::byte>)>                      m_sink;
    std::vector<std::byte>                                               m_last;
    plexus::detail::move_only_function<void(std::span<const std::byte>)> m_on_data;
    plexus::detail::move_only_function<void()>                           m_on_closed;
    plexus::detail::move_only_function<void(plexus::io::io_error)>       m_on_error;
    plexus::detail::move_only_function<void(plexus::wire::close_cause)>  m_on_protocol_close;
};

// Each session derives keys from a salt that varies per (run, session-instance): a
// reconnect/restart mints a fresh schedule, so the (key, nonce) space is partitioned
// by session even when the per-direction counters restart at 0.
inline derived_keys keys_for(std::uint32_t session_salt)
{
    std::vector<std::byte> psk;
    for(char c : std::string{"a-shared-pre-shared-key-of-decent-length"})
        psk.push_back(static_cast<std::byte>(static_cast<unsigned char>(c)));
    std::array<std::byte, 16> in_nonce{};
    std::array<std::byte, 16> rs_nonce{};
    std::array<std::byte, 32> transcript{};
    for(std::size_t i = 0; i < 16; ++i)
    {
        in_nonce[i] = static_cast<std::byte>((session_salt + i) & 0xffu);
        rs_nonce[i] = static_cast<std::byte>((session_salt * 7u + i) & 0xffu);
    }
    for(std::size_t i = 0; i < 32; ++i)
        transcript[i] = static_cast<std::byte>((session_salt * 13u + i) & 0xffu);
    derived_keys k{};
    REQUIRE(derive_keys(psk, in_nonce, rs_nonce, transcript, k));
    return k;
}

inline std::vector<std::byte> make_frame(std::uint64_t session_id, std::string_view payload)
{
    plexus::wire::frame_header hdr{};
    hdr.type         = plexus::wire::msg_type::unidirectional;
    hdr.flags        = 0;
    hdr.session_id   = session_id;
    hdr.timestamp_ns = 1;
    hdr.payload_len  = payload.size();
    std::vector<std::byte> pt;
    for(char c : payload)
        pt.push_back(static_cast<std::byte>(static_cast<unsigned char>(c)));
    return plexus::wire::encode_frame(hdr, pt);
}

// (key fingerprint, epoch, sequence, direction). The key fingerprint stands in for the
// key identity: distinct keys give distinct fingerprints, so a repeated tuple is a
// genuine (key, nonce) reuse.
using nonce_tuple = std::tuple<std::uint64_t, std::uint32_t, std::uint64_t, int>;

inline std::uint64_t key_fingerprint(std::uint32_t session_salt, std::uint32_t epoch, int direction)
{
    // Each (session, epoch, direction) names a distinct derived key in the construction
    // under test; fold them into one value so the uniqueness set keys on key identity.
    return (static_cast<std::uint64_t>(session_salt) << 40) ^
            (static_cast<std::uint64_t>(epoch) << 8) ^ static_cast<std::uint64_t>(direction);
}

// Drive `count` sends through a sender keyed by `session_salt`, recording each emitted
// (key, epoch, sequence, direction) tuple into `seen`. The pre-send (epoch, sequence)
// reads ARE the nonce the decorator constructs for that frame.
inline void drive_flow(std::set<nonce_tuple> &seen, std::uint32_t session_salt, std::uint64_t count,
                       std::uint64_t rekey_threshold)
{
    wire_lower                        wire;
    authenticated_channel<wire_lower> sender(wire, aead_cipher_id::chacha20_poly1305,
                                             keys_for(session_salt), 0, rekey_threshold);
    const auto                        frame = make_frame(session_salt, "n");
    for(std::uint64_t i = 0; i < count; ++i)
    {
        const auto epoch = sender.send_epoch();
        const auto seq   = sender.send_sequence();
        const auto fp    = key_fingerprint(session_salt, epoch, /*direction=*/0);
        const bool fresh = seen.insert(nonce_tuple{fp, epoch, seq, 0}).second;
        REQUIRE(fresh); // no (key, nonce) reuse, ever
        sender.send(frame);
    }
}

}

#endif
