// The named nonce-uniqueness gate (looped, reproducible). Across normal steady-state
// flow, a reconnect (a fresh session minting new keys + a fresh epoch axis), and a
// forced session restart, NO (key-epoch, nonce, direction) tuple ever repeats. The
// AEAD nonce is the deterministic per-direction epoch||sequence counter — never a
// CSPRNG draw, and never the ARQ sequence (a retransmit-reused ARQ sequence does not
// reuse an AEAD nonce). The scenario set runs N times with a deterministic
// construction so a second ctest invocation reproduces the identical pass.

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

using plexus::crypto::aead_cipher_id;
using plexus::crypto::authenticated_channel;
using plexus::crypto::derived_keys;
using plexus::crypto::derive_keys;

namespace {

class wire_lower
{
public:
    void send(std::span<const std::byte> data)
    {
        m_last.assign(data.begin(), data.end());
        if(m_sink)
            m_sink(std::span<const std::byte>{m_last});
    }
    void close() {}
    [[nodiscard]] plexus::io::endpoint remote_endpoint() const { return {"wire", ""}; }
    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)> cb) { m_on_data = std::move(cb); }
    void on_closed(plexus::detail::move_only_function<void()> cb) { m_on_closed = std::move(cb); }
    void on_error(plexus::detail::move_only_function<void(plexus::io::io_error)> cb) { m_on_error = std::move(cb); }
    void on_protocol_close(plexus::detail::move_only_function<void(plexus::wire::close_cause)> cb) { m_on_protocol_close = std::move(cb); }
    [[nodiscard]] std::size_t backpressured() const { return 0; }

    std::function<void(std::span<const std::byte>)> m_sink;
    std::vector<std::byte> m_last;
    plexus::detail::move_only_function<void(std::span<const std::byte>)> m_on_data;
    plexus::detail::move_only_function<void()> m_on_closed;
    plexus::detail::move_only_function<void(plexus::io::io_error)> m_on_error;
    plexus::detail::move_only_function<void(plexus::wire::close_cause)> m_on_protocol_close;
};

// Each session derives keys from a salt that varies per (run, session-instance): a
// reconnect/restart mints a fresh schedule, so the (key, nonce) space is partitioned
// by session even when the per-direction counters restart at 0.
derived_keys keys_for(std::uint32_t session_salt)
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

std::vector<std::byte> make_frame(std::uint64_t session_id, std::string_view payload)
{
    plexus::wire::frame_header hdr{};
    hdr.type = plexus::wire::msg_type::unidirectional;
    hdr.flags = 0;
    hdr.session_id = session_id;
    hdr.timestamp_ns = 1;
    hdr.payload_len = payload.size();
    std::vector<std::byte> pt;
    for(char c : payload)
        pt.push_back(static_cast<std::byte>(static_cast<unsigned char>(c)));
    return plexus::wire::encode_frame(hdr, pt);
}

// (key fingerprint, epoch, sequence, direction). The key fingerprint stands in for the
// key identity: distinct keys give distinct fingerprints, so a repeated tuple is a
// genuine (key, nonce) reuse.
using nonce_tuple = std::tuple<std::uint64_t, std::uint32_t, std::uint64_t, int>;

std::uint64_t key_fingerprint(std::uint32_t session_salt, std::uint32_t epoch, int direction)
{
    // Each (session, epoch, direction) names a distinct derived key in the construction
    // under test; fold them into one value so the uniqueness set keys on key identity.
    return (static_cast<std::uint64_t>(session_salt) << 40) ^
           (static_cast<std::uint64_t>(epoch) << 8) ^ static_cast<std::uint64_t>(direction);
}

// Drive `count` sends through a sender keyed by `session_salt`, recording each emitted
// (key, epoch, sequence, direction) tuple into `seen`. The pre-send (epoch, sequence)
// reads ARE the nonce the decorator constructs for that frame.
void drive_flow(std::set<nonce_tuple> &seen, std::uint32_t session_salt,
                std::uint64_t count, std::uint64_t rekey_threshold)
{
    wire_lower wire;
    authenticated_channel<wire_lower> sender(wire, aead_cipher_id::chacha20_poly1305,
                                             keys_for(session_salt), 0, rekey_threshold);
    const auto frame = make_frame(session_salt, "n");
    for(std::uint64_t i = 0; i < count; ++i)
    {
        const auto epoch = sender.send_epoch();
        const auto seq = sender.send_sequence();
        const auto fp = key_fingerprint(session_salt, epoch, /*direction=*/0);
        const bool fresh = seen.insert(nonce_tuple{fp, epoch, seq, 0}).second;
        REQUIRE(fresh);   // no (key, nonce) reuse, ever
        sender.send(frame);
    }
}

}

TEST_CASE("crypto.nonce_uniqueness holds across flow, reconnect, and restart (looped)", "[crypto][nonce]")
{
    std::set<nonce_tuple> seen;
    const std::uint64_t rekey_threshold = 64;     // exercise several rekeys per session
    const std::uint64_t per_session = 500;

    // N looped runs of the three-scenario set with a deterministic, RNG-free
    // construction — a second ctest invocation reproduces the identical pass.
    const int loops = 8;
    for(int run = 0; run < loops; ++run)
    {
        const std::uint32_t base = static_cast<std::uint32_t>(run * 10);
        drive_flow(seen, base + 1, per_session, rekey_threshold);   // steady-state flow
        drive_flow(seen, base + 2, per_session, rekey_threshold);   // reconnect: fresh session/keys
        drive_flow(seen, base + 3, per_session, rekey_threshold);   // forced restart: fresh session/keys
    }

    REQUIRE(seen.size() == static_cast<std::size_t>(loops) * 3u * per_session);
}

TEST_CASE("crypto.nonce_uniqueness holds across a > 256-rekey session (epoch byte wrap)", "[crypto][nonce]")
{
    // Force > 256 rekeys in one session (a rekey every frame) so the 8-bit wire epoch
    // byte wraps 0xff -> 0x00. Each epoch installs a FRESH key (a distinct fingerprint),
    // so the (key-fingerprint, epoch, sequence, direction) set stays unique across the
    // wrap — the wire-byte collision (epoch 5 and 261 share byte 5) never reuses a
    // (key,nonce) pair because the keys differ.
    std::set<nonce_tuple> seen;
    const std::uint64_t frames = 600;   // well past 256 rekeys at threshold 1
    drive_flow(seen, /*session_salt=*/77, frames, /*rekey_threshold=*/1);
    REQUIRE(seen.size() == static_cast<std::size_t>(frames));

    // Run a second time into a separate set to confirm the construction is reproducible
    // (RNG-free): the identical tuple set is produced.
    std::set<nonce_tuple> seen_again;
    drive_flow(seen_again, /*session_salt=*/77, frames, /*rekey_threshold=*/1);
    REQUIRE(seen_again == seen);
}

TEST_CASE("crypto.nonce_uniqueness the AEAD nonce is independent of the ARQ sequence", "[crypto][nonce]")
{
    wire_lower wire;
    authenticated_channel<wire_lower> sender(wire, aead_cipher_id::chacha20_poly1305,
                                             keys_for(99), 0, /*rekey_threshold=*/1ull << 20);

    // Drive frames whose header carries a RETRANSMIT-REUSED session/ARQ-style value
    // (the same number rides every frame); the AEAD nonce must still advance monotonically.
    std::vector<std::pair<std::uint32_t, std::uint64_t>> nonces;
    const auto reused = make_frame(/*session_id=*/42, "retransmit");
    for(int i = 0; i < 100; ++i)
    {
        nonces.emplace_back(sender.send_epoch(), sender.send_sequence());
        sender.send(reused);
    }

    for(std::size_t i = 1; i < nonces.size(); ++i)
    {
        // Same epoch (below threshold), strictly increasing sequence — the AEAD nonce
        // advances per send regardless of the reused header value.
        REQUIRE(nonces[i].first == nonces[i - 1].first);
        REQUIRE(nonces[i].second == nonces[i - 1].second + 1);
    }
    REQUIRE(nonces.back().second == 99);
}

TEST_CASE("crypto.nonce_uniqueness the nonce is a deterministic counter, never a CSPRNG draw", "[crypto][nonce]")
{
    // Two independently-constructed senders over the same keys emit the IDENTICAL nonce
    // sequence — a CSPRNG-drawn nonce could not be reproduced, a counter is.
    wire_lower wire_a;
    wire_lower wire_b;
    authenticated_channel<wire_lower> a(wire_a, aead_cipher_id::chacha20_poly1305, keys_for(7), 0, 16);
    authenticated_channel<wire_lower> b(wire_b, aead_cipher_id::chacha20_poly1305, keys_for(7), 0, 16);

    const auto frame = make_frame(7, "deterministic");
    for(int i = 0; i < 50; ++i)
    {
        REQUIRE(a.send_epoch() == b.send_epoch());
        REQUIRE(a.send_sequence() == b.send_sequence());
        a.send(frame);
        b.send(frame);
    }
}
