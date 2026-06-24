#include "test_datagram_replay_common.h"

using namespace datagram_replay_fixture;

TEST_CASE("crypto.datagram_replay delivers an out-of-order but fresh datagram", "[crypto][datagram_replay]")
{
    const auto keys = fixed_keys();
    const auto wire = seal_datagrams(keys, 5);

    wire_lower recv_wire;
    datagram_authenticated_channel<wire_lower> receiver(recv_wire, aead_cipher_id::chacha20_poly1305, swapped(keys));

    std::vector<std::vector<std::byte>> delivered;
    receiver.on_data([&](std::span<const std::byte> f) { delivered.emplace_back(f.begin(), f.end()); });

    // Reordered arrival: 2, 0, 4, 1, 3 — every fresh datagram opens within the window.
    for(std::size_t i : {std::size_t{2}, std::size_t{0}, std::size_t{4}, std::size_t{1}, std::size_t{3}})
        recv_wire.feed(std::span<const std::byte>{wire[i]});

    REQUIRE(delivered.size() == 5);
    REQUIRE(receiver.dropped_count() == 0);
    REQUIRE(delivered[0] == make_frame(7, "datagram-payload-2"));
    REQUIRE(delivered[3] == make_frame(7, "datagram-payload-1"));
}

TEST_CASE("crypto.datagram_replay drops and counts a replayed datagram with no event", "[crypto][datagram_replay]")
{
    const auto keys = fixed_keys();
    const auto wire = seal_datagrams(keys, 3);

    wire_lower recv_wire;
    datagram_authenticated_channel<wire_lower> receiver(recv_wire, aead_cipher_id::chacha20_poly1305, swapped(keys));

    bool protocol_closed = false;
    receiver.on_protocol_close([&](plexus::wire::close_cause) { protocol_closed = true; });

    int delivered = 0;
    receiver.on_data([&](std::span<const std::byte>) { ++delivered; });

    recv_wire.feed(std::span<const std::byte>{wire[0]});
    recv_wire.feed(std::span<const std::byte>{wire[1]});
    recv_wire.feed(std::span<const std::byte>{wire[0]}); // replay of seq 0
    recv_wire.feed(std::span<const std::byte>{wire[1]}); // replay of seq 1

    REQUIRE(delivered == 2);
    REQUIRE(receiver.replay_count() == 2);
    REQUIRE(receiver.tamper_dropped_count() == 0);
    REQUIRE_FALSE(protocol_closed);
    REQUIRE_FALSE(recv_wire.m_closed);
}

TEST_CASE("crypto.datagram_replay drops and counts a bad-tag datagram without teardown", "[crypto][datagram_replay]")
{
    const auto keys = fixed_keys();
    auto wire       = seal_datagrams(keys, 2);

    wire_lower recv_wire;
    datagram_authenticated_channel<wire_lower> receiver(recv_wire, aead_cipher_id::chacha20_poly1305, swapped(keys));

    bool protocol_closed = false;
    bool errored         = false;
    receiver.on_protocol_close([&](plexus::wire::close_cause) { protocol_closed = true; });
    recv_wire.on_error([&](plexus::io::io_error) { errored = true; });

    int delivered = 0;
    receiver.on_data([&](std::span<const std::byte>) { ++delivered; });

    // Flip a byte in the sealed ciphertext+tag region of the first datagram.
    wire[0].back() ^= std::byte{0xff};
    recv_wire.feed(std::span<const std::byte>{wire[0]});
    recv_wire.feed(std::span<const std::byte>{wire[1]}); // the honest datagram still opens

    REQUIRE(delivered == 1);
    REQUIRE(receiver.tamper_dropped_count() == 1);
    REQUIRE(receiver.replay_count() == 0);
    REQUIRE_FALSE(protocol_closed);
    REQUIRE_FALSE(recv_wire.m_closed);
}
