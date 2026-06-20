#include "test_datagram_replay_common.h"

using namespace datagram_replay_fixture;

TEST_CASE("crypto.anti_replay_window would_accept predicts check_and_set without mutating",
          "[crypto][anti_replay]")
{
    // would_accept must return the SAME verdict check_and_set would, while leaving the
    // window untouched (so a pre-auth probe cannot itself slide the window).
    plexus::crypto::anti_replay_window<> window;

    const std::array<std::uint64_t, 9> seqs{5, 9, 9, 7, 3, 200, 200, 8, 1};
    for(std::uint64_t s : seqs)
    {
        const std::uint64_t before = window.highest();
        // Probing (twice) must predict the next check_and_set without mutating: the
        // window's highest is unchanged by the probe, and a repeated probe is stable.
        const auto p1 = window.would_accept(s);
        const auto p2 = window.would_accept(s);
        REQUIRE(p1 == p2);
        REQUIRE(window.highest() == before);
        REQUIRE(p1 == window.check_and_set(s));
    }
    REQUIRE(window.highest() == 200);
}

TEST_CASE("crypto.datagram_replay a forged huge-sequence datagram does not wedge the window",
          "[crypto][datagram_replay]")
{
    const auto keys = fixed_keys();

    // Two identical runs prove the forged-packet rejection is reproducible.
    for(int run = 0; run < 2; ++run)
    {
        auto wire = seal_datagrams(keys, 2);

        wire_lower                                 recv_wire;
        datagram_authenticated_channel<wire_lower> receiver(
                recv_wire, aead_cipher_id::chacha20_poly1305, swapped(keys));

        bool protocol_closed = false;
        receiver.on_protocol_close([&](plexus::wire::close_cause) { protocol_closed = true; });

        std::vector<std::vector<std::byte>> delivered;
        receiver.on_data([&](std::span<const std::byte> f)
                         { delivered.emplace_back(f.begin(), f.end()); });

        // Forge a datagram in the current epoch with seq = 2^64-1 and a corrupted tag.
        // Pre-fix, would_accept's slide ran before the tag check, advancing the window
        // to the maximum so every later legitimate datagram became reject_old.
        auto forged = wire[0];
        set_wire_seq(forged, std::numeric_limits<std::uint64_t>::max());
        forged.back() ^= std::byte{0xff};

        recv_wire.feed(std::span<const std::byte>{forged});
        REQUIRE(delivered.empty());
        REQUIRE(receiver.tamper_dropped_count() == 1);

        // A legitimate in-window datagram AFTER the forgery still opens — the window
        // was NOT advanced to max by the forged huge sequence.
        recv_wire.feed(std::span<const std::byte>{wire[0]});
        recv_wire.feed(std::span<const std::byte>{wire[1]});
        REQUIRE(delivered.size() == 2);
        REQUIRE(delivered[0] == make_frame(7, "datagram-payload-0"));
        REQUIRE_FALSE(protocol_closed);
    }
}

TEST_CASE("crypto.datagram_replay a forged next-epoch datagram does not desync the key",
          "[crypto][datagram_replay]")
{
    const auto keys = fixed_keys();

    for(int run = 0; run < 2; ++run)
    {
        auto wire = seal_datagrams(keys, 3);

        wire_lower                                 recv_wire;
        datagram_authenticated_channel<wire_lower> receiver(
                recv_wire, aead_cipher_id::chacha20_poly1305, swapped(keys));

        std::vector<std::vector<std::byte>> delivered;
        receiver.on_data([&](std::span<const std::byte> f)
                         { delivered.emplace_back(f.begin(), f.end()); });

        // Forge a datagram carrying the NEXT epoch byte (current is 0) with a corrupted
        // tag. Pre-fix, select_recv_epoch derived/advanced the recv key and reset the
        // window before open, discarding the only key that opens real current-epoch
        // traffic — one packet permanently desynced the session.
        auto forged = wire[0];
        forged[8]   = static_cast<std::byte>(0x01); // next epoch byte
        forged.back() ^= std::byte{0xff};

        recv_wire.feed(std::span<const std::byte>{forged});
        REQUIRE(delivered.empty());
        REQUIRE(receiver.tamper_dropped_count() == 1);

        // A legitimate current-epoch datagram after the forgery still opens — the recv
        // epoch/key were NOT advanced by the forged next-epoch byte.
        recv_wire.feed(std::span<const std::byte>{wire[0]});
        recv_wire.feed(std::span<const std::byte>{wire[1]});
        REQUIRE(delivered.size() == 2);
        REQUIRE(delivered[0] == make_frame(7, "datagram-payload-0"));
    }
}
