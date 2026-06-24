#include "test_authenticated_channel_common.h"

using namespace authenticated_channel_fixture;

TEST_CASE("crypto.authenticated_channel round-trips a plaintext header-on frame", "[crypto][authenticated_channel]")
{
    const auto                        keys = fixed_keys();
    wire_lower                        send_wire;
    wire_lower                        recv_wire;
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
    const auto                        keys = fixed_keys();
    wire_lower                        send_wire;
    wire_lower                        recv_wire;
    authenticated_channel<wire_lower> sender(send_wire, aead_cipher_id::chacha20_poly1305, keys);
    authenticated_channel<wire_lower> receiver(recv_wire, aead_cipher_id::chacha20_poly1305, swapped(keys));

    bool protocol_closed = false;
    bool errored         = false;
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
    const auto                        keys = fixed_keys();
    wire_lower                        send_wire;
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
    const std::uint64_t               threshold = 4;
    authenticated_channel<wire_lower> sender(send_wire, aead_cipher_id::chacha20_poly1305, keys, 0, threshold);
    authenticated_channel<wire_lower> receiver(recv_wire, aead_cipher_id::chacha20_poly1305, swapped(keys), 0, threshold);

    std::vector<std::vector<std::byte>> on_wire;
    send_wire.m_sink = [&](std::span<const std::byte> b) { on_wire.emplace_back(b.begin(), b.end()); };

    const auto frame = make_frame(7, "straddle");
    for(std::uint64_t i = 0; i < threshold; ++i)
        sender.send(frame); // fills epoch 0, seq 0..3
    REQUIRE(sender.send_epoch() == 0);

    sender.send(frame); // crosses the threshold → epoch 1, seq 0
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
    REQUIRE(receiver.send_epoch() == 0); // the recv-side advance does not touch the send epoch
}
