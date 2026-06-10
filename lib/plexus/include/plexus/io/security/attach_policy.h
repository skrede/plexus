#ifndef HPP_GUARD_PLEXUS_IO_SECURITY_ATTACH_POLICY_H
#define HPP_GUARD_PLEXUS_IO_SECURITY_ATTACH_POLICY_H

#include "plexus/io/security/attach_facts.h"
#include "plexus/io/security/cookie_secret.h"
#include "plexus/io/security/ct_equal.h"

#include <span>
#include <array>
#include <vector>
#include <cstddef>
#include <utility>
#include <stdexcept>

namespace plexus::io::security {

// The injectable attach-admission contract — mechanism, not policy. The handshake
// bridge consults an attach_policy to admit or refuse an attaching peer over the
// flat attach_facts VALUE. decide() is non-throwing and runs on the FSM's
// non-throwing gate path. A no-PSK node injects accept_any (an explicit choice,
// never a null silent-accept).
class attach_policy
{
public:
    virtual ~attach_policy() = default;

    [[nodiscard]] virtual bool decide(const attach_facts &facts) const noexcept = 0;
};

// One keyed pre-shared secret: the opaque id the handshake carries and the raw key
// material the proof is recomputed under.
struct keyed_psk
{
    std::array<std::byte, k_key_id_len> key_id;
    std::vector<std::byte>              material;
};

// The default attach_policy: a PSK keystore. decide() looks the key up by id (an
// absent/removed key refuses without dereferencing), recomputes the reflection-proof
// challenge-response MAC under the injected hmac_fn, and finishes with the
// constant-time compare. An empty keystore refuses everything (fail-closed, mirroring
// spki_pin_policy).
class psk_keystore_policy final : public attach_policy
{
public:
    // The minimum raw PSK length, enforced fail-fast at injection. Passphrase->key
    // derivation is the consumer's job above plexus.
    static constexpr std::size_t k_min_psk_len = 16;

    psk_keystore_policy(std::vector<keyed_psk> keys, hmac_fn hmac)
        : m_keys(std::move(keys))
        , m_hmac(std::move(hmac))
    {
        for(const auto &k : m_keys)
            if(k.material.size() < k_min_psk_len)
                throw std::runtime_error("psk_keystore_policy: key material below minimum length");
    }

    [[nodiscard]] bool decide(const attach_facts &facts) const noexcept override
    {
        const keyed_psk *key = lookup(facts.key_id);
        if(key == nullptr)
            return false;
        std::array<std::byte, 32> expected{};
        if(!recompute_proof(*key, facts, expected))
            return false;
        return ct_equal(expected, facts.proof);
    }

private:
    [[nodiscard]] const keyed_psk *lookup(const std::array<std::byte, k_key_id_len> &id) const noexcept
    {
        for(const auto &k : m_keys)
            if(k.key_id == id)
                return &k;
        return nullptr;
    }

    // The proof input binds a fixed label, the role byte, both node-ids, both nonces,
    // and the transcript digest in that fixed order: the role byte makes the two
    // directions compute distinct MACs (anti-reflection), and the transcript digest
    // makes a downgraded cipher offer change the MAC (anti-downgrade).
    [[nodiscard]] bool recompute_proof(const keyed_psk &key, const attach_facts &f,
                                       std::span<std::byte> out) const noexcept
    {
        static constexpr std::array<std::byte, 13> label{
            std::byte{'p'}, std::byte{'l'}, std::byte{'e'}, std::byte{'x'}, std::byte{'u'},
            std::byte{'s'}, std::byte{'-'}, std::byte{'a'}, std::byte{'t'}, std::byte{'t'},
            std::byte{'a'}, std::byte{'c'}, std::byte{'h'}};
        std::vector<std::byte> msg;
        msg.reserve(label.size() + 1 + f.initiator_id.size() + f.responder_id.size() +
                    f.peer_nonce.size() + f.own_nonce.size() + f.transcript_digest.size());
        msg.insert(msg.end(), label.begin(), label.end());
        msg.push_back(static_cast<std::byte>(f.role));
        msg.insert(msg.end(), f.initiator_id.begin(), f.initiator_id.end());
        msg.insert(msg.end(), f.responder_id.begin(), f.responder_id.end());
        msg.insert(msg.end(), f.peer_nonce.begin(), f.peer_nonce.end());
        msg.insert(msg.end(), f.own_nonce.begin(), f.own_nonce.end());
        msg.insert(msg.end(), f.transcript_digest.begin(), f.transcript_digest.end());
        return m_hmac(key.material, msg, out);
    }

    std::vector<keyed_psk> m_keys;

    // The injected MAC is invoked from the const decide() path; the C++20 fallback
    // move_only_function has a non-const call operator, so the seam holds it mutable
    // (mirroring cookie_secret).
    mutable hmac_fn m_hmac;
};

// The explicit no-PSK default: admit every peer. The mechanism/policy split — a node
// that requires no attach proof injects this rather than leaving the seam null.
class accept_any final : public attach_policy
{
public:
    [[nodiscard]] bool decide(const attach_facts &) const noexcept override { return true; }
};

}

#endif
