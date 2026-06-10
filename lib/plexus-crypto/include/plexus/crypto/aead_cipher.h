#ifndef HPP_GUARD_PLEXUS_CRYPTO_AEAD_CIPHER_H
#define HPP_GUARD_PLEXUS_CRYPTO_AEAD_CIPHER_H

#include <span>
#include <array>
#include <vector>
#include <cstdint>
#include <cstddef>

namespace plexus::crypto {

// 256-bit AEAD key (both ChaCha20-Poly1305 and AES-256-GCM take a 32-byte key).
using aead_key = std::array<std::byte, 32>;

// RFC 8439 nonce width (96-bit) and the Poly1305/GCM tag width (128-bit).
constexpr std::size_t k_aead_nonce_len = 12;
constexpr std::size_t k_aead_tag_len = 16;

enum class aead_cipher_id : std::uint8_t
{
    chacha20_poly1305,
    aes_256_gcm
};

// seal writes ciphertext then appends the 16-byte tag into `out` (reused scratch);
// out.size() == plaintext.size() + k_aead_tag_len on success. `aad` is authenticated
// but not encrypted (the plaintext frame_header the stack above must read in the clear).
[[nodiscard]] bool seal(aead_cipher_id cipher, const aead_key &key,
                        std::span<const std::byte, k_aead_nonce_len> nonce,
                        std::span<const std::byte> aad, std::span<const std::byte> plaintext,
                        std::vector<std::byte> &out);

// open verifies the appended tag (EVP's constant-time check) and writes the recovered
// plaintext into `out`; returns false on any verification failure (a flipped ciphertext
// byte, a flipped tag byte, wrong aad, or wrong nonce). On a failure `out` is cleared, so
// a caller never reads attacker-controlled, unverified plaintext from a rejected packet.
[[nodiscard]] bool open(aead_cipher_id cipher, const aead_key &key,
                        std::span<const std::byte, k_aead_nonce_len> nonce,
                        std::span<const std::byte> aad,
                        std::span<const std::byte> ciphertext_and_tag,
                        std::vector<std::byte> &out);

}

#endif
