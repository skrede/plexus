#ifndef HPP_GUARD_PLEXUS_IO_SECURITY_ATTACH_POLICY_H
#define HPP_GUARD_PLEXUS_IO_SECURITY_ATTACH_POLICY_H

#include "plexus/io/security/attach_facts.h"
#include "plexus/io/security/cookie_secret.h"
#include "plexus/io/security/ct_equal.h"
#include "plexus/detail/fail_closed.h"

#include <span>
#include <array>
#include <vector>
#include <cstddef>
#include <utility>

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
                plexus::detail::fail_closed("psk_keystore_policy: key material below minimum length");
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
    [[nodiscard]] const keyed_psk *
    lookup(const std::array<std::byte, k_key_id_len> &id) const noexcept
    {
        for(const auto &k : m_keys)
            if(k.key_id == id)
                return &k;
        return nullptr;
    }

    // MAC the canonical proof input (assembled by attach_proof_input — the layout the
    // prover reproduces byte for byte) under this key's material.
    [[nodiscard]] bool recompute_proof(const keyed_psk &key, const attach_facts &f,
                                       std::span<std::byte> out) const noexcept
    {
        return m_hmac(key.material, attach_proof_input(f), out);
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

// The attaching side's counterpart to psk_keystore_policy: the prover holds its OWN
// keyed PSK and the SAME hmac_fn the verifier uses, and stamps a proof the verifier
// recomputes byte for byte. prove() MACs the canonical attach_proof_input(facts) under
// the held material — the single-sourced layout — so the prover and the policy can
// never drift. A disengaged prover (no material) leaves the proof field unused on the
// accept-any plaintext path.
class attach_prover
{
public:
    attach_prover() = default;

    attach_prover(keyed_psk key, hmac_fn hmac)
            : m_key(std::move(key))
            , m_hmac(std::move(hmac))
            , m_engaged(true)
    {
    }

    [[nodiscard]] bool engaged() const noexcept { return m_engaged; }

    [[nodiscard]] const std::array<std::byte, k_key_id_len> &key_id() const noexcept
    {
        return m_key.key_id;
    }

    // Stamp the proof for `facts` into `out` (32 bytes). The role/ids/nonces/transcript
    // in `facts` are the prover's OWN view (its role, its own_nonce, the peer's nonce as
    // peer_nonce), so the verifier — recomputing under the same key with its mirror view
    // — matches. Returns false on a degraded MAC so the caller fails closed.
    [[nodiscard]] bool prove(const attach_facts &facts, std::span<std::byte> out) const
    {
        return m_hmac(m_key.material, attach_proof_input(facts), out);
    }

private:
    keyed_psk m_key;

    // Invoked from the const prove() path; the C++20 fallback move_only_function has a
    // non-const call operator, so the seam holds the MAC mutable (mirroring cookie_secret).
    mutable hmac_fn m_hmac;
    bool            m_engaged{false};
};

}

#endif
