#ifndef HPP_GUARD_PLEXUS_IO_SECURITY_VERIFY_POLICY_H
#define HPP_GUARD_PLEXUS_IO_SECURITY_VERIFY_POLICY_H

#include "plexus/io/security/cert_facts.h"

#include <array>
#include <vector>
#include <cstddef>

namespace plexus::io::security {

// The injectable peer-verification contract — mechanism, not policy. The backend
// verify callback consults a verify_policy to make the accept/reject decision over
// the backend-extracted cert_facts VALUE (never a raw cert handle / DER buffer): the
// core decides, the backend only supplies the parsed facts. An engine-agnostic seam —
// a future non-default engine fills the same cert_facts and reaches the same decide().
//
// decide() is non-throwing and returns the accept verdict. The credential REQUIRES a
// policy injection — a credential with no policy fails closed (the callback rejects).
// There is no verify_none path.
class verify_policy
{
public:
    virtual ~verify_policy() = default;

    // True iff this peer is authorized given the extracted facts. Non-throwing.
    [[nodiscard]] virtual bool decide(const cert_facts &facts) const noexcept = 0;
};

// The default verify_policy: an SPKI-fingerprint allowlist. Accepts iff the peer's
// full 32-byte SHA-256 SPKI digest is in the pinned set. An empty allowlist accepts
// nothing (fail-closed — this INVERTS the prior art, which treated an empty pin list
// as accept-any). The decision is a pure value comparison over facts.spki_sha256.
class spki_pin_policy final : public verify_policy
{
public:
    explicit spki_pin_policy(std::vector<std::array<std::byte, 32>> pinned) noexcept
        : m_pinned(std::move(pinned))
    {
    }

    [[nodiscard]] bool decide(const cert_facts &facts) const noexcept override
    {
        for(const auto &pin : m_pinned)
            if(pin == facts.spki_sha256)
                return true;
        return false;
    }

private:
    std::vector<std::array<std::byte, 32>> m_pinned;
};

}

#endif
