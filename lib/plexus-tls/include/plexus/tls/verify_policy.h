#ifndef HPP_GUARD_PLEXUS_TLS_VERIFY_POLICY_H
#define HPP_GUARD_PLEXUS_TLS_VERIFY_POLICY_H

#include "plexus/tls/spki_fingerprint.h"

#include <span>
#include <vector>
#include <cstddef>

namespace plexus::tls {

// The injectable peer-verification contract — mechanism, not policy. The OpenSSL
// verify callback consults a verify_policy (reached via SSL_CTX ex_data) to make
// the accept/reject decision on the peer's leaf cert. The contract is pure bytes
// (the DER-encoded leaf), engine-agnostic: a future non-OpenSSL engine satisfies
// the same verify(leaf_der) seam without exposing any OpenSSL type.
//
// verify() is non-throwing and returns the accept decision. The credential
// REQUIRES a policy injection — a credential with no policy fails closed (the
// callback returns reject). There is no verify_none path.
class verify_policy
{
public:
    virtual ~verify_policy() = default;

    // True iff this peer's leaf is authorized. Non-throwing.
    [[nodiscard]] virtual bool verify(std::span<const std::byte> leaf_der) const noexcept = 0;
};

// The default verify_policy: an SPKI-fingerprint allowlist. Accepts iff the
// peer leaf's full 32-byte SHA-256 SPKI digest is in the pinned set. An empty
// allowlist accepts nothing (fail-closed — this INVERTS the prior art, which
// treated an empty pin list as accept-any). The DER→digest extraction lives in
// the crypto TU; this header carries only the pinned set.
class spki_pin_policy final : public verify_policy
{
public:
    explicit spki_pin_policy(std::vector<spki_digest> pinned) noexcept
        : m_pinned(std::move(pinned))
    {
    }

    [[nodiscard]] bool verify(std::span<const std::byte> leaf_der) const noexcept override;

private:
    std::vector<spki_digest> m_pinned;
};

}

#endif
