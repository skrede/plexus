#include "test_datagram_replay_common.h"

using namespace datagram_replay_fixture;

TEST_CASE("crypto.datagram_replay round-trips across the 256-epoch boundary", "[crypto][datagram_replay]")
{
    const auto keys = fixed_keys();

    wire_lower recv_wire;
    datagram_authenticated_channel<wire_lower> receiver(recv_wire, aead_cipher_id::chacha20_poly1305, swapped(keys));

    std::vector<std::byte> delivered;
    receiver.on_data([&](std::span<const std::byte> f) { delivered.assign(f.begin(), f.end()); });

    // The datagram decorator advances the recv epoch one step per new wire epoch byte
    // (forward-deriving the key). Drive a sender forward through > 256 epochs — each
    // epoch sealed with the matching forward-derived key at initial_epoch = e — so the
    // wire epoch byte wraps 0xff -> 0x00 and the seal/open nonce epoch field must agree.
    plexus::crypto::aead_key send_key = keys.k_send; // epoch 0 send key
    const std::uint32_t epochs        = 600;
    for(std::uint32_t e = 0; e < epochs; ++e)
    {
        wire_lower send_wire;
        derived_keys ek{.k_send = send_key, .k_recv = keys.k_recv};
        datagram_authenticated_channel<wire_lower> sender(send_wire, aead_cipher_id::chacha20_poly1305, ek, e);

        std::vector<std::byte> on_wire;
        send_wire.m_sink = [&](std::span<const std::byte> b) { on_wire.assign(b.begin(), b.end()); };
        const auto frame = make_frame(7, "datagram-epoch-" + std::to_string(e));
        sender.send(frame);

        delivered.clear();
        recv_wire.feed(std::span<const std::byte>{on_wire});
        // Opens at every epoch, including across the 0xff->0x00 wrap.
        REQUIRE(delivered == frame);

        send_key = step_forward(send_key);
    }
    REQUIRE(receiver.tamper_dropped_count() == 0);
    REQUIRE(receiver.replay_count() == 0);
}

TEST_CASE("crypto.datagram_replay drops a datagram below the window floor as too-old", "[crypto][datagram_replay]")
{
    const auto keys               = fixed_keys();
    const std::size_t past_window = plexus::datagram::k_anti_replay_window_bits + 8;
    const auto wire               = seal_datagrams(keys, past_window + 1);

    wire_lower recv_wire;
    datagram_authenticated_channel<wire_lower> receiver(recv_wire, aead_cipher_id::chacha20_poly1305, swapped(keys));

    std::int32_t delivered = 0;
    receiver.on_data([&](std::span<const std::byte>) { ++delivered; });

    // Accept the highest sequence first, then feed a far-older one (below the floor).
    recv_wire.feed(std::span<const std::byte>{wire[past_window]});
    recv_wire.feed(std::span<const std::byte>{wire[0]});

    REQUIRE(delivered == 1);
    REQUIRE(receiver.replay_count() == 1);
}

namespace {

std::vector<std::vector<std::byte>> send_header_only(datagram_authenticated_channel<wire_lower> &sender, wire_lower &wire, std::uint64_t count, std::uint64_t keep_from)
{
    std::uint64_t index = 0;
    std::vector<std::vector<std::byte>> kept;
    wire.m_sink = [&](std::span<const std::byte> b)
    {
        if(index++ >= keep_from)
            kept.emplace_back(b.begin(), b.end());
    };
    const auto frame = make_frame(7, "");
    for(std::uint64_t i = 0; i < count; ++i)
        sender.send(frame);
    wire.m_sink = nullptr;
    return kept;
}

}

TEST_CASE("crypto.datagram_replay rekeys its send direction at the usage threshold and the receiver follows", "[crypto][datagram_replay]")
{
    using plexus::crypto::k_rekey_message_threshold;
    const auto keys = fixed_keys();

    wire_lower send_wire;
    datagram_authenticated_channel<wire_lower> sender(send_wire, aead_cipher_id::chacha20_poly1305, keys);
    const auto kept = send_header_only(sender, send_wire, k_rekey_message_threshold + 1, k_rekey_message_threshold - 1);

    REQUIRE(kept.size() == 2);
    REQUIRE(kept[1][8] == std::byte{1});
    REQUIRE(std::vector<std::byte>(kept[1].begin(), kept[1].begin() + 8) == std::vector<std::byte>(8));

    wire_lower recv_wire;
    datagram_authenticated_channel<wire_lower> receiver(recv_wire, aead_cipher_id::chacha20_poly1305, swapped(keys));
    std::int32_t delivered = 0;
    receiver.on_data([&](std::span<const std::byte>) { ++delivered; });
    for(const auto &datagram : kept)
        recv_wire.feed(std::span<const std::byte>{datagram});
    REQUIRE(delivered == 2);
    REQUIRE(receiver.tamper_dropped_count() == 0);
}

TEST_CASE("crypto.datagram_replay rekeys every two sends and the receiver follows each epoch", "[crypto][datagram_replay]")
{
    const auto keys = fixed_keys();

    wire_lower send_wire;
    datagram_authenticated_channel<wire_lower> sender(send_wire, aead_cipher_id::chacha20_poly1305, keys, 0, 2);
    const auto kept = send_header_only(sender, send_wire, 6, 0);

    REQUIRE(kept.size() == 6);
    for(std::size_t i = 0; i < kept.size(); ++i)
        REQUIRE(kept[i][8] == static_cast<std::byte>(i / 2));

    wire_lower recv_wire;
    datagram_authenticated_channel<wire_lower> receiver(recv_wire, aead_cipher_id::chacha20_poly1305, swapped(keys));
    std::int32_t delivered = 0;
    receiver.on_data([&](std::span<const std::byte>) { ++delivered; });
    for(const auto &datagram : kept)
        recv_wire.feed(std::span<const std::byte>{datagram});
    REQUIRE(delivered == 6);
    REQUIRE(receiver.tamper_dropped_count() == 0);
}
