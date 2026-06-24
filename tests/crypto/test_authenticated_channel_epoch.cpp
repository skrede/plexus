#include "test_authenticated_channel_common.h"

using namespace authenticated_channel_fixture;

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

    const std::uint32_t frames = 600; // crosses epoch 255 -> 256 -> ... well past the wrap
    for(std::uint32_t i = 0; i < frames; ++i)
    {
        delivered.clear();
        const auto frame = make_frame(7, "epoch-crossing-" + std::to_string(i));
        sender.send(frame);
        REQUIRE(delivered == frame); // every frame opens, including across the 0xff->0x00 wrap
    }
    REQUIRE_FALSE(closed);
    REQUIRE(sender.send_epoch() > 256);
    REQUIRE(receiver.send_epoch() == 0); // the recv-side advance does not touch the send epoch
}

TEST_CASE("crypto.authenticated_channel fragment budget subtracts the AEAD overhead", "[crypto][authenticated_channel]")
{
    using plexus::io::effective_fragment_budget;
    using plexus::io::k_aead_fragment_overhead;

    const std::size_t transport_budget = 1400;
    const auto plain                   = effective_fragment_budget(transport_budget, false);
    const auto aead                    = effective_fragment_budget(transport_budget, true);

    REQUIRE(k_aead_fragment_overhead == 8 + 1 + 16);
    REQUIRE(aead == plain - k_aead_fragment_overhead);

    // A payload sized to the AEAD budget seals (ciphertext == payload size), gains the
    // per-fragment seq + epoch + tag, and exactly fits inside the plaintext budget.
    REQUIRE(aead + k_aead_fragment_overhead == plain);
}
