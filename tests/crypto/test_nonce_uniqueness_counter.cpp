#include "test_nonce_uniqueness_common.h"

using namespace nonce_uniqueness_fixture;

TEST_CASE("crypto.nonce_uniqueness the AEAD nonce is independent of the ARQ sequence", "[crypto][nonce]")
{
    wire_lower                        wire;
    authenticated_channel<wire_lower> sender(wire, aead_cipher_id::chacha20_poly1305, keys_for(99), 0, /*rekey_threshold=*/1ull << 20);

    // Drive frames whose header carries a RETRANSMIT-REUSED session/ARQ-style value
    // (the same number rides every frame); the AEAD nonce must still advance monotonically.
    std::vector<std::pair<std::uint32_t, std::uint64_t>> nonces;
    const auto                                           reused = make_frame(/*session_id=*/42, "retransmit");
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
    wire_lower                        wire_a;
    wire_lower                        wire_b;
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
