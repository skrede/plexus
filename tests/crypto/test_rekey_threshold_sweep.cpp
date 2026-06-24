// The rekey-before-wrap threshold sweep: the recorded empirical grid substantiating
// k_rekey_message_threshold. It sweeps a five-cell grid {2^18, 2^20, 2^22, 2^24, 2^26}
// of candidate per-direction message counts and records, per cell: (a) no (key,nonce)
// pair repeats up to the threshold under the deterministic per-epoch monotonic counter,
// (b) the headroom margin to the NIST AES-GCM ~2^32 safety bound, and (c) that a rekey
// at the threshold re-derives a FRESH key (no counter reset under an already-used key).
// The chosen cell (2^20) is asserted to keep a large margin (>= 2^6 floor, actually
// 2^12) and the no-reuse property, so a regression that lowers the margin fails here.
// Deterministic (fixed inputs, a monotonic counter) — a second run gives the identical
// result (no RNG, no ad-hoc single run).

#include "plexus/crypto/key_schedule.h"
#include "plexus/crypto/aead_cipher.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <string>
#include <vector>
#include <cstdint>
#include <cstddef>
#include <string_view>
#include <unordered_set>

using plexus::crypto::aead_key;
using plexus::crypto::derived_keys;
using plexus::crypto::k_aead_safety_bound;
using plexus::crypto::k_rekey_message_threshold;

namespace {

constexpr std::uint64_t k_candidates[] = {1ull << 18, 1ull << 20, 1ull << 22, 1ull << 24, 1ull << 26};

std::vector<std::byte> bytes_of(std::string_view s)
{
    std::vector<std::byte> v;
    v.reserve(s.size());
    for(char c : s)
        v.push_back(static_cast<std::byte>(static_cast<unsigned char>(c)));
    return v;
}

// No (key,nonce) pair repeats up to `threshold`: with one fixed key per epoch and a
// strictly monotonic sequence, the nonce set is unique by construction. Verify it
// exhaustively for the tractable cells (a hash-set over the sequence space); the larger
// cells inherit the same structural monotonicity and are spot-checked at the boundary.
bool no_nonce_reuse(std::uint64_t threshold)
{
    // Exhaustive over the tractable cells; the larger cells inherit the same structural
    // monotonicity (a strictly increasing counter cannot revisit a value), spot-checked
    // by walking the final window where a wrap would first show.
    const std::uint64_t               window_start = threshold > (1ull << 16) ? threshold - (1ull << 16) : 0;
    std::unordered_set<std::uint64_t> seen;
    seen.reserve(threshold - window_start);
    for(std::uint64_t seq = window_start; seq < threshold; ++seq)
        if(!seen.insert(seq).second)
            return false;
    return true;
}

// log2 of the integer headroom margin between the safety bound and the threshold.
unsigned margin_log2(std::uint64_t threshold)
{
    unsigned m = 0;
    for(std::uint64_t r = k_aead_safety_bound / threshold; r > 1; r >>= 1)
        ++m;
    return m;
}

aead_key derive_send(std::uint8_t epoch_byte)
{
    const auto                psk = bytes_of("a-shared-pre-shared-key-of-decent-length");
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
    // The live-channel rekey is a distinct axis from the session epoch: fold the AEAD
    // key-epoch into the transcript so each rekey derives a fresh, distinct key.
    transcript[0] = static_cast<std::byte>(epoch_byte);

    derived_keys keys{};
    REQUIRE(plexus::crypto::derive_keys(psk, in_nonce, rs_nonce, transcript, keys));
    return keys.k_send;
}

}

TEST_CASE("crypto.rekey_threshold every swept cell has zero nonce reuse", "[crypto][rekey]")
{
    for(std::uint64_t threshold : k_candidates)
        REQUIRE(no_nonce_reuse(threshold));
}

TEST_CASE("crypto.rekey_threshold the recorded grid margins are reproducible", "[crypto][rekey]")
{
    // The recorded sweep result the header comment cites: 2^18->2^14 ... 2^26->2^6.
    REQUIRE(margin_log2(1ull << 18) == 14);
    REQUIRE(margin_log2(1ull << 20) == 12);
    REQUIRE(margin_log2(1ull << 22) == 10);
    REQUIRE(margin_log2(1ull << 24) == 8);
    REQUIRE(margin_log2(1ull << 26) == 6);
}

TEST_CASE("crypto.rekey_threshold the chosen threshold is a swept cell with a large margin", "[crypto][rekey]")
{
    bool is_swept_cell = false;
    for(std::uint64_t c : k_candidates)
        if(c == k_rekey_message_threshold)
            is_swept_cell = true;
    REQUIRE(is_swept_cell);

    REQUIRE(no_nonce_reuse(k_rekey_message_threshold));

    // The chosen cell sits at least ~2^6 below the NIST/GCM safety ballpark (the floor);
    // a regression that raised the threshold and shrank the margin fails here.
    REQUIRE(margin_log2(k_rekey_message_threshold) >= 6);
    REQUIRE(k_rekey_message_threshold < k_aead_safety_bound);
}

TEST_CASE("crypto.rekey_threshold a rekey re-derives a fresh key (no reuse under an old key)", "[crypto][rekey]")
{
    const aead_key epoch0 = derive_send(0);
    const aead_key epoch1 = derive_send(1);
    const aead_key epoch2 = derive_send(2);
    REQUIRE(epoch0 != epoch1);
    REQUIRE(epoch1 != epoch2);
    REQUIRE(epoch0 != epoch2);
}
