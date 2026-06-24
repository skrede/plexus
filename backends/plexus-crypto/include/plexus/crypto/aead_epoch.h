#ifndef HPP_GUARD_PLEXUS_CRYPTO_AEAD_EPOCH_H
#define HPP_GUARD_PLEXUS_CRYPTO_AEAD_EPOCH_H

#include "plexus/crypto/key_schedule.h"
#include "plexus/crypto/aead_cipher.h"

#include <span>
#include <array>
#include <cstddef>
#include <cstdint>

namespace plexus::crypto {

// One deterministic rekey step: re-derive from the retiring key as IKM under a fixed
// rekey label. Both directions run the identical chain so a receiver tracks the sender's
// epoch by re-deriving, never by exchanging fresh key material.
inline bool derive_forward(const aead_key &from, aead_key &out)
{
    std::array<std::byte, 16> nonce{};
    std::array<std::byte, 32> transcript{};
    derived_keys              d{};
    if(!derive_keys(std::span<const std::byte>{from}, nonce, nonce, transcript, d))
        return false;
    out = d.k_send;
    return true;
}

// 96-bit RFC 8439 nonce = epoch(4 BE) || sequence(8 BE). The epoch field commits to the
// 8-bit wire epoch byte on BOTH seal and open (only that byte rides the wire), so the high
// three epoch bytes are always zero and the two sides agree past epoch 255. Each epoch
// installs a fresh key, so an 8-bit epoch disambiguator is sufficient for (key,nonce)
// uniqueness within a key's lifetime; the per-direction sequence guarantees within-epoch
// uniqueness.
inline std::array<std::byte, k_aead_nonce_len> make_nonce(std::uint32_t epoch, std::uint64_t seq) noexcept
{
    std::array<std::byte, k_aead_nonce_len> n{};
    for(std::size_t i = 0; i < 4; ++i)
        n[i] = static_cast<std::byte>((epoch >> (8u * (3u - i))) & 0xffu);
    for(std::size_t i = 0; i < 8; ++i)
        n[4 + i] = static_cast<std::byte>((seq >> (8u * (7u - i))) & 0xffu);
    return n;
}

}

#endif
