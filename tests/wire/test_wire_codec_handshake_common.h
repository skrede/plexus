#ifndef HPP_GUARD_PLEXUS_TESTS_WIRE_TEST_WIRE_CODEC_HANDSHAKE_COMMON_H
#define HPP_GUARD_PLEXUS_TESTS_WIRE_TEST_WIRE_CODEC_HANDSHAKE_COMMON_H

#include "test_wire_codec_common.h"

namespace wire_codec_handshake_fixture {

inline std::array<std::byte, 16> id_filled(std::uint8_t value)
{
    std::array<std::byte, 16> id{};
    id.fill(std::byte{value});
    return id;
}

inline std::array<std::byte, 16> id_mixed_high_bit()
{
    std::array<std::byte, 16> id{};
    for(std::size_t i = 0; i < id.size(); ++i)
        id[i] = std::byte{static_cast<std::uint8_t>(i % 2 == 0 ? 0x80 : 0x7F)};
    return id;
}

inline std::array<std::byte, 16> id_distinct()
{
    std::array<std::byte, 16> id{};
    for(std::size_t i = 0; i < id.size(); ++i)
        id[i] = std::byte{static_cast<std::uint8_t>(0x10 + i)};
    return id;
}

inline std::array<std::byte, 8> key_id_seed()
{
    std::array<std::byte, 8> k{};
    for(std::size_t i = 0; i < k.size(); ++i)
        k[i] = static_cast<std::byte>(0x60 + i);
    return k;
}

inline std::array<std::byte, 16> nonce_seed()
{
    std::array<std::byte, 16> n{};
    for(std::size_t i = 0; i < n.size(); ++i)
        n[i] = static_cast<std::byte>(0x70 + i);
    return n;
}

inline std::array<std::byte, k_handshake_proof_len> proof_seed(std::uint8_t base = 0x90)
{
    std::array<std::byte, k_handshake_proof_len> pr{};
    for(std::size_t i = 0; i < pr.size(); ++i)
        pr[i] = static_cast<std::byte>(base + i);
    return pr;
}

inline handshake_request make_request(const std::array<std::byte, 16> &id)
{
    return handshake_request{.id                       = id,
                             .version_major            = 0x11,
                             .version_minor            = 0x22,
                             .compatible_version_major = 0x33,
                             .compatible_version_minor = 0x44,
                             .protocol_version         = 0x55,
                             .fingerprint              = 0x0123456789ABCDEFull,
                             .key_id                   = key_id_seed(),
                             .own_nonce                = nonce_seed(),
                             .cipher_offer             = cipher_offer_bits::chacha20_poly1305 | cipher_offer_bits::aes_256_gcm,
                             .chosen_cipher            = cipher_offer_bits::chacha20_poly1305,
                             .proof                    = proof_seed()};
}

inline handshake_response make_response(const std::array<std::byte, 16> &id, handshake_status status)
{
    return handshake_response{.id                       = id,
                              .version_major            = 0x11,
                              .version_minor            = 0x22,
                              .compatible_version_major = 0x33,
                              .compatible_version_minor = 0x44,
                              .protocol_version         = 0x55,
                              .fingerprint              = 0xFEDCBA9876543210ull,
                              .key_id                   = key_id_seed(),
                              .own_nonce                = nonce_seed(),
                              .cipher_offer             = cipher_offer_bits::chacha20_poly1305 | cipher_offer_bits::aes_256_gcm,
                              .chosen_cipher            = cipher_offer_bits::aes_256_gcm,
                              .proof                    = proof_seed(0xC1),
                              .status                   = status};
}

inline void check_request_equal(const handshake_request &a, const handshake_request &b)
{
    CHECK(a.id == b.id);
    CHECK(a.version_major == b.version_major);
    CHECK(a.version_minor == b.version_minor);
    CHECK(a.compatible_version_major == b.compatible_version_major);
    CHECK(a.compatible_version_minor == b.compatible_version_minor);
    CHECK(a.protocol_version == b.protocol_version);
    CHECK(a.fingerprint == b.fingerprint);
    CHECK(a.key_id == b.key_id);
    CHECK(a.own_nonce == b.own_nonce);
    CHECK(a.cipher_offer == b.cipher_offer);
    CHECK(a.chosen_cipher == b.chosen_cipher);
    CHECK(a.proof == b.proof);
}

}

#endif
