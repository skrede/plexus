#ifndef HPP_GUARD_PLEXUS_TESTS_IO_TEST_ATTACH_POLICY_COMMON_H
#define HPP_GUARD_PLEXUS_TESTS_IO_TEST_ATTACH_POLICY_COMMON_H

// The attach_policy oracle: a pure sans-OpenSSL drive of the PSK attach-admission
// decision over INJECTED fake hmac_fn (no OpenSSL, no backend link — plexus::plexus
// only; the litmus proof the decision needs no crypto lib).

#include "plexus/io/security/attach_policy.h"
#include "plexus/io/security/attach_facts.h"
#include "plexus/io/security/ct_equal.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <vector>
#include <cstddef>
#include <utility>

using plexus::io::security::accept_any;
using plexus::io::security::attach_facts;
using plexus::io::security::attach_role;
using plexus::io::security::ct_equal;
using plexus::io::security::hmac_fn;
using plexus::io::security::keyed_psk;
using plexus::io::security::k_key_id_len;
using plexus::io::security::psk_keystore_policy;

namespace attach_policy_fixture {

// A deterministic position-weighted multiply-accumulate over key||msg (a stand-in for
// HMAC; the only property the decision needs is determinism over its inputs). No
// OpenSSL. Mirrors the cookie_secret oracle's fake_hmac.
inline hmac_fn fake_hmac()
{
    return [](std::span<const std::byte> key, std::span<const std::byte> msg,
              std::span<std::byte> out)
    {
        if(out.size() != 32)
            return false;
        for(std::size_t i = 0; i < out.size(); ++i)
        {
            unsigned acc = 0x811c9dc5u + static_cast<unsigned>(i);
            for(std::size_t k = 0; k < key.size(); ++k)
                acc = (acc ^ std::to_integer<unsigned>(key[k])) * 0x01000193u +
                        static_cast<unsigned>(k);
            for(std::size_t m = 0; m < msg.size(); ++m)
                acc = (acc ^ std::to_integer<unsigned>(msg[m])) * 0x01000193u +
                        static_cast<unsigned>(m + i);
            out[i] = static_cast<std::byte>(acc & 0xffu);
        }
        return true;
    };
}

inline std::array<std::byte, k_key_id_len> key_id_of(std::uint8_t v)
{
    std::array<std::byte, k_key_id_len> id{};
    id.fill(std::byte{v});
    return id;
}

inline std::vector<std::byte> material_of(std::uint8_t seed, std::size_t len = 16)
{
    std::vector<std::byte> m(len);
    for(std::size_t i = 0; i < len; ++i)
        m[i] = static_cast<std::byte>((seed + i) & 0xff);
    return m;
}

inline attach_facts facts_for(std::uint8_t key_seed, attach_role role)
{
    attach_facts f;
    f.key_id = key_id_of(key_seed);
    f.initiator_id.fill(std::byte{0x10});
    f.responder_id.fill(std::byte{0x20});
    f.peer_nonce.fill(std::byte{0x30});
    f.own_nonce.fill(std::byte{0x40});
    f.transcript_digest.fill(std::byte{0x50});
    f.role = role;
    return f;
}

// Recompute the canonical proof for `facts` under `material` via the same fake HMAC
// the policy uses, so a facts value can be presented with a VALID proof. The proof
// input ordering is the policy's contract; this mirror keeps it in one place.
inline std::array<std::byte, 32> proof_for(std::span<const std::byte> material,
                                           const attach_facts        &f)
{
    static constexpr std::array<std::byte, 13> label{
            std::byte{'p'}, std::byte{'l'}, std::byte{'e'}, std::byte{'x'}, std::byte{'u'},
            std::byte{'s'}, std::byte{'-'}, std::byte{'a'}, std::byte{'t'}, std::byte{'t'},
            std::byte{'a'}, std::byte{'c'}, std::byte{'h'}};
    std::vector<std::byte> msg;
    msg.insert(msg.end(), label.begin(), label.end());
    msg.push_back(static_cast<std::byte>(f.role));
    msg.insert(msg.end(), f.initiator_id.begin(), f.initiator_id.end());
    msg.insert(msg.end(), f.responder_id.begin(), f.responder_id.end());
    msg.insert(msg.end(), f.peer_nonce.begin(), f.peer_nonce.end());
    msg.insert(msg.end(), f.own_nonce.begin(), f.own_nonce.end());
    msg.insert(msg.end(), f.transcript_digest.begin(), f.transcript_digest.end());
    std::array<std::byte, 32> out{};
    fake_hmac()(material, msg, out);
    return out;
}

}

#endif
