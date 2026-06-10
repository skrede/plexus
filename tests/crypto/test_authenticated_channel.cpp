// The authenticated_channel proof: a header-on frame sealed by the send-side
// decorator is delivered as the original plaintext header-on frame by the
// receive-side decorator; a flipped sealed byte fires on_protocol_close (NOT
// on_error) and closes; the send nonce sequence is a monotonic per-direction
// counter (never reset under a live key, never a CSPRNG draw); a rekey at the
// threshold re-derives a fresh key, bumps the key-epoch, and straddles an
// in-flight pre-rekey frame; and the fragment budget subtracts the AEAD tag.

#include "plexus/crypto/authenticated_channel.h"
#include "plexus/crypto/key_schedule.h"
#include "plexus/crypto/aead_cipher.h"

#include "plexus/io/fragmentation.h"
#include "plexus/io/byte_channel.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/frame.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
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

// A test lower byte_channel: send() forwards the bytes through an injected sink
// (set by the test to drive the peer decorator's lower on_data), and on_data is
// the seam the decorator wires to receive bytes.
class wire_lower
{
public:
    void send(std::span<const std::byte> data)
    {
        m_last.assign(data.begin(), data.end());
        if(m_sink)
            m_sink(std::span<const std::byte>{m_last});
    }
    void close() { m_closed = true; }
    [[nodiscard]] plexus::io::endpoint remote_endpoint() const { return {"wire", ""}; }
    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)> cb) { m_on_data = std::move(cb); }
    void on_closed(plexus::detail::move_only_function<void()> cb) { m_on_closed = std::move(cb); }
    void on_error(plexus::detail::move_only_function<void(plexus::io::io_error)> cb) { m_on_error = std::move(cb); }
    void on_protocol_close(plexus::detail::move_only_function<void(plexus::wire::close_cause)> cb) { m_on_protocol_close = std::move(cb); }
    [[nodiscard]] std::size_t backpressured() const { return 0; }

    void feed(std::span<const std::byte> bytes)
    {
        if(m_on_data)
            m_on_data(bytes);
    }

    std::function<void(std::span<const std::byte>)> m_sink;
    std::vector<std::byte> m_last;
    bool m_closed{false};
    plexus::detail::move_only_function<void(std::span<const std::byte>)> m_on_data;
    plexus::detail::move_only_function<void()> m_on_closed;
    plexus::detail::move_only_function<void(plexus::io::io_error)> m_on_error;
    plexus::detail::move_only_function<void(plexus::wire::close_cause)> m_on_protocol_close;
};

static_assert(plexus::io::byte_channel<wire_lower>,
    "wire_lower must satisfy byte_channel for the decorator test");

derived_keys fixed_keys()
{
    std::vector<std::byte> psk;
    for(char c : std::string{"a-shared-pre-shared-key-of-decent-length"})
        psk.push_back(static_cast<std::byte>(static_cast<unsigned char>(c)));
    std::array<std::byte, 16> in_nonce{};
    std::array<std::byte, 16> rs_nonce{};
    std::array<std::byte, 32> transcript{};
    for(std::size_t i = 0; i < 16; ++i)
    {
        in_nonce[i] = static_cast<std::byte>(0x10 + i);
        rs_nonce[i] = static_cast<std::byte>(0xa0 + i);
    }
    for(std::size_t i = 0; i < 32; ++i)
        transcript[i] = static_cast<std::byte>(0x40 + i);
    derived_keys k{};
    REQUIRE(derive_keys(psk, in_nonce, rs_nonce, transcript, k));
    return k;
}

derived_keys swapped(const derived_keys &k)
{
    return derived_keys{.k_send = k.k_recv, .k_recv = k.k_send};
}

std::vector<std::byte> make_frame(std::uint64_t session_id, std::string_view payload)
{
    plexus::wire::frame_header hdr{};
    hdr.type = plexus::wire::msg_type::unidirectional;
    hdr.flags = 0;
    hdr.session_id = session_id;
    hdr.timestamp_ns = 123456;
    hdr.payload_len = payload.size();
    std::vector<std::byte> pt;
    for(char c : payload)
        pt.push_back(static_cast<std::byte>(static_cast<unsigned char>(c)));
    return plexus::wire::encode_frame(hdr, pt);
}

}

TEST_CASE("crypto.authenticated_channel round-trips a plaintext header-on frame", "[crypto][authenticated_channel]")
{
    const auto keys = fixed_keys();
    wire_lower send_wire;
    wire_lower recv_wire;
    authenticated_channel<wire_lower> sender(send_wire, aead_cipher_id::chacha20_poly1305, keys);
    authenticated_channel<wire_lower> receiver(recv_wire, aead_cipher_id::chacha20_poly1305, swapped(keys));

    send_wire.m_sink = [&](std::span<const std::byte> b) { recv_wire.feed(b); };

    std::vector<std::byte> delivered;
    receiver.on_data([&](std::span<const std::byte> f) { delivered.assign(f.begin(), f.end()); });

    const auto frame = make_frame(7, "hello plaintext header-on world");
    sender.send(frame);

    REQUIRE(delivered == frame);
}

TEST_CASE("crypto.authenticated_channel a tampered frame fires on_protocol_close, not on_error", "[crypto][authenticated_channel]")
{
    const auto keys = fixed_keys();
    wire_lower send_wire;
    wire_lower recv_wire;
    authenticated_channel<wire_lower> sender(send_wire, aead_cipher_id::chacha20_poly1305, keys);
    authenticated_channel<wire_lower> receiver(recv_wire, aead_cipher_id::chacha20_poly1305, swapped(keys));

    bool protocol_closed = false;
    bool errored = false;
    receiver.on_protocol_close([&](plexus::wire::close_cause) { protocol_closed = true; });
    recv_wire.on_error([&](plexus::io::io_error) { errored = true; });

    std::vector<std::byte> on_wire;
    send_wire.m_sink = [&](std::span<const std::byte> b) { on_wire.assign(b.begin(), b.end()); };

    const auto frame = make_frame(7, "tamper me");
    sender.send(frame);

    // Flip a byte in the sealed ciphertext region (past the epoch byte + header).
    on_wire.back() ^= std::byte{0xff};
    recv_wire.feed(std::span<const std::byte>{on_wire});

    REQUIRE(protocol_closed);
    REQUIRE_FALSE(errored);
    REQUIRE(recv_wire.m_closed);
}

TEST_CASE("crypto.authenticated_channel send nonce sequence is a monotonic counter", "[crypto][authenticated_channel]")
{
    const auto keys = fixed_keys();
    wire_lower send_wire;
    authenticated_channel<wire_lower> sender(send_wire, aead_cipher_id::chacha20_poly1305, keys);

    REQUIRE(sender.send_sequence() == 0);
    const auto frame = make_frame(7, "x");
    for(std::uint64_t i = 1; i <= 1000; ++i)
    {
        sender.send(frame);
        REQUIRE(sender.send_sequence() == i);
    }
    // The epoch is unchanged below the rekey threshold (no spurious reset under a live key).
    REQUIRE(sender.send_epoch() == 0);
}

TEST_CASE("crypto.authenticated_channel rekeys at the threshold and straddles an in-flight frame", "[crypto][authenticated_channel]")
{
    const auto keys = fixed_keys();
    wire_lower send_wire;
    wire_lower recv_wire;
    // A small rekey threshold so a rekey is exercised behaviorally without sealing 2^20
    // frames; the threshold is a real configurable knob, not a test backdoor.
    const std::uint64_t threshold = 4;
    authenticated_channel<wire_lower> sender(send_wire, aead_cipher_id::chacha20_poly1305, keys, 0, threshold);
    authenticated_channel<wire_lower> receiver(recv_wire, aead_cipher_id::chacha20_poly1305, swapped(keys), 0, threshold);

    std::vector<std::vector<std::byte>> on_wire;
    send_wire.m_sink = [&](std::span<const std::byte> b) { on_wire.emplace_back(b.begin(), b.end()); };

    const auto frame = make_frame(7, "straddle");
    for(std::uint64_t i = 0; i < threshold; ++i)
        sender.send(frame);                     // fills epoch 0, seq 0..3
    REQUIRE(sender.send_epoch() == 0);

    sender.send(frame);                         // crosses the threshold → epoch 1, seq 0
    REQUIRE(sender.send_epoch() == 1);

    // The last-but-one frame names epoch 0; the last names epoch 1.
    REQUIRE(static_cast<std::uint8_t>(on_wire[threshold - 1].front()) == 0u);
    REQUIRE(static_cast<std::uint8_t>(on_wire[threshold].front()) == 1u);

    std::vector<std::byte> delivered;
    receiver.on_data([&](std::span<const std::byte> f) { delivered.assign(f.begin(), f.end()); });

    // Deliver in order: the receiver opens epoch-0 frames, then advances to epoch 1 on
    // the post-rekey frame — every frame round-trips, no (key,nonce) confusion.
    for(const auto &w : on_wire)
    {
        delivered.clear();
        recv_wire.feed(std::span<const std::byte>{w});
        REQUIRE(delivered == frame);
    }
    REQUIRE(receiver.send_epoch() == 0);   // the recv-side advance does not touch the send epoch
}

TEST_CASE("crypto.authenticated_channel round-trips across the 256-epoch boundary", "[crypto][authenticated_channel]")
{
    const auto keys = fixed_keys();
    wire_lower send_wire;
    wire_lower recv_wire;
    // A rekey every frame so a few hundred sends cross epoch 256 (the wire epoch byte
    // wraps 0xff -> 0x00) — the seal/open nonce epoch field must agree past 255.
    const std::uint64_t threshold = 1;
    authenticated_channel<wire_lower> sender(send_wire, aead_cipher_id::chacha20_poly1305, keys, 0, threshold);
    authenticated_channel<wire_lower> receiver(recv_wire, aead_cipher_id::chacha20_poly1305, swapped(keys), 0, threshold);

    std::vector<std::byte> delivered;
    bool closed = false;
    receiver.on_data([&](std::span<const std::byte> f) { delivered.assign(f.begin(), f.end()); });
    receiver.on_protocol_close([&](plexus::wire::close_cause) { closed = true; });
    send_wire.m_sink = [&](std::span<const std::byte> b) { recv_wire.feed(b); };

    const std::uint32_t frames = 600;   // crosses epoch 255 -> 256 -> ... well past the wrap
    for(std::uint32_t i = 0; i < frames; ++i)
    {
        delivered.clear();
        const auto frame = make_frame(7, "epoch-crossing-" + std::to_string(i));
        sender.send(frame);
        REQUIRE(delivered == frame);   // every frame opens, including across the 0xff->0x00 wrap
    }
    REQUIRE_FALSE(closed);
    REQUIRE(sender.send_epoch() > 256);
    REQUIRE(receiver.send_epoch() == 0);   // the recv-side advance does not touch the send epoch
}

TEST_CASE("crypto.authenticated_channel fragment budget subtracts the AEAD tag", "[crypto][authenticated_channel]")
{
    using plexus::io::effective_fragment_budget;
    using plexus::io::k_aead_tag_overhead;

    const std::size_t transport_budget = 1400;
    const auto plain = effective_fragment_budget(transport_budget, false);
    const auto aead = effective_fragment_budget(transport_budget, true);

    REQUIRE(k_aead_tag_overhead == 16);
    REQUIRE(aead == plain - k_aead_tag_overhead);

    // A payload sized to the AEAD budget seals (ciphertext == payload size) + the tag and
    // exactly fits inside the plaintext budget — no oversize at the edge.
    REQUIRE(aead + k_aead_tag_overhead == plain);
}
