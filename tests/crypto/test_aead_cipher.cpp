// The AEAD primitive proof: seal/open round-trips plaintext under ChaCha20-Poly1305
// (default) and AES-256-GCM (alternate) with AAD + a 96-bit nonce + a 16-byte tag;
// open returns false on a flipped ciphertext byte, a flipped tag byte, a wrong AAD,
// or a wrong nonce. Plus the HKDF key schedule: distinct k_send/k_recv from one
// master (per-direction separation) and a transcript-bound derivation (a changed
// transcript_digest changes the keys).

#include "plexus/crypto/key_schedule.h"
#include "plexus/crypto/aead_cipher.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <algorithm>

using plexus::crypto::aead_cipher_id;
using plexus::crypto::aead_key;
using plexus::crypto::derived_keys;
using plexus::crypto::k_aead_nonce_len;
using plexus::crypto::k_aead_tag_len;

namespace {

std::vector<std::byte> bytes_of(std::string_view s)
{
    std::vector<std::byte> v;
    v.reserve(s.size());
    for(char c : s)
        v.push_back(static_cast<std::byte>(static_cast<unsigned char>(c)));
    return v;
}

aead_key fixed_key(std::uint8_t seed)
{
    aead_key k{};
    for(std::size_t i = 0; i < k.size(); ++i)
        k[i] = static_cast<std::byte>((seed * 7u + i) & 0xffu);
    return k;
}

std::array<std::byte, k_aead_nonce_len> fixed_nonce(std::uint8_t seed)
{
    std::array<std::byte, k_aead_nonce_len> n{};
    for(std::size_t i = 0; i < n.size(); ++i)
        n[i] = static_cast<std::byte>((seed * 3u + i) & 0xffu);
    return n;
}

void round_trips(aead_cipher_id cipher)
{
    const auto key = fixed_key(11);
    const auto nonce = fixed_nonce(5);
    const auto aad = bytes_of("plexus header bytes");
    const auto pt = bytes_of("the quick brown fox jumps over the lazy dog");

    std::vector<std::byte> sealed;
    REQUIRE(plexus::crypto::seal(cipher, key, nonce, aad, pt, sealed));
    REQUIRE(sealed.size() == pt.size() + k_aead_tag_len);

    std::vector<std::byte> recovered;
    REQUIRE(plexus::crypto::open(cipher, key, nonce, aad, sealed, recovered));
    REQUIRE(recovered == pt);
}

void rejects_tampering(aead_cipher_id cipher)
{
    const auto key = fixed_key(22);
    const auto nonce = fixed_nonce(9);
    const auto aad = bytes_of("aad");
    const auto pt = bytes_of("confidential payload");

    std::vector<std::byte> sealed;
    REQUIRE(plexus::crypto::seal(cipher, key, nonce, aad, pt, sealed));

    std::vector<std::byte> out;

    auto flipped_cipher = sealed;
    flipped_cipher.front() ^= std::byte{0xff};
    REQUIRE_FALSE(plexus::crypto::open(cipher, key, nonce, aad, flipped_cipher, out));

    auto flipped_tag = sealed;
    flipped_tag.back() ^= std::byte{0xff};
    REQUIRE_FALSE(plexus::crypto::open(cipher, key, nonce, aad, flipped_tag, out));

    const auto wrong_aad = bytes_of("AAD");
    REQUIRE_FALSE(plexus::crypto::open(cipher, key, nonce, wrong_aad, sealed, out));

    const auto wrong_nonce = fixed_nonce(10);
    REQUIRE_FALSE(plexus::crypto::open(cipher, key, wrong_nonce, aad, sealed, out));
}

}

TEST_CASE("crypto.aead_cipher round-trips under ChaCha20-Poly1305", "[crypto][aead]")
{
    round_trips(aead_cipher_id::chacha20_poly1305);
}

TEST_CASE("crypto.aead_cipher round-trips under AES-256-GCM", "[crypto][aead]")
{
    round_trips(aead_cipher_id::aes_256_gcm);
}

TEST_CASE("crypto.aead_cipher open rejects every tamper class (ChaCha20-Poly1305)", "[crypto][aead]")
{
    rejects_tampering(aead_cipher_id::chacha20_poly1305);
}

TEST_CASE("crypto.aead_cipher open rejects every tamper class (AES-256-GCM)", "[crypto][aead]")
{
    rejects_tampering(aead_cipher_id::aes_256_gcm);
}

TEST_CASE("crypto.aead_cipher seals an empty plaintext to exactly the tag", "[crypto][aead]")
{
    const auto key = fixed_key(3);
    const auto nonce = fixed_nonce(1);
    const auto aad = bytes_of("h");
    const std::vector<std::byte> empty;

    std::vector<std::byte> sealed;
    REQUIRE(plexus::crypto::seal(aead_cipher_id::chacha20_poly1305, key, nonce, aad, empty, sealed));
    REQUIRE(sealed.size() == k_aead_tag_len);

    std::vector<std::byte> out;
    REQUIRE(plexus::crypto::open(aead_cipher_id::chacha20_poly1305, key, nonce, aad, sealed, out));
    REQUIRE(out.empty());
}

TEST_CASE("crypto.key_schedule derives distinct send and recv keys", "[crypto][key_schedule]")
{
    const auto psk = bytes_of("a-shared-pre-shared-key-of-decent-length");
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

    derived_keys keys{};
    REQUIRE(plexus::crypto::derive_keys(psk, in_nonce, rs_nonce, transcript, keys));
    REQUIRE(keys.k_send != keys.k_recv);
}

TEST_CASE("crypto.key_schedule binds the transcript digest into the keys", "[crypto][key_schedule]")
{
    const auto psk = bytes_of("a-shared-pre-shared-key-of-decent-length");
    std::array<std::byte, 16> in_nonce{};
    std::array<std::byte, 16> rs_nonce{};
    std::array<std::byte, 32> transcript_a{};
    std::array<std::byte, 32> transcript_b{};
    for(std::size_t i = 0; i < 16; ++i)
    {
        in_nonce[i] = static_cast<std::byte>(0x10 + i);
        rs_nonce[i] = static_cast<std::byte>(0xa0 + i);
    }
    for(std::size_t i = 0; i < 32; ++i)
    {
        transcript_a[i] = static_cast<std::byte>(0x40 + i);
        transcript_b[i] = static_cast<std::byte>(0x41 + i);
    }

    derived_keys keys_a{};
    derived_keys keys_b{};
    REQUIRE(plexus::crypto::derive_keys(psk, in_nonce, rs_nonce, transcript_a, keys_a));
    REQUIRE(plexus::crypto::derive_keys(psk, in_nonce, rs_nonce, transcript_b, keys_b));
    REQUIRE(keys_a.k_send != keys_b.k_send);
    REQUIRE(keys_a.k_recv != keys_b.k_recv);
}
