#ifndef HPP_GUARD_PLEXUS_TLS_TLS_CREDENTIAL_H
#define HPP_GUARD_PLEXUS_TLS_TLS_CREDENTIAL_H

#include "plexus/tls/verify_policy.h"

#include <memory>
#include <string>

// Forward-declared OpenSSL handle: the header never sees a complete SSL_CTX —
// only the .cpp TU includes <openssl/ssl.h>. This keeps OpenSSL out of every
// consumer's translation unit (the same fwd-declare-in-header / include-in-cpp
// split a compiled adapter uses), so the crypto stays behind the seam.
struct ssl_ctx_st;

namespace plexus::tls {

// The minimum TLS protocol floor a credential negotiates. Default 1.3 for the
// CA-less peer mesh; 1.2 stays reachable as a construction parameter.
enum class tls_version
{
    v1_2,
    v1_3,
};

// Move-only RAII owner of an OpenSSL SSL_CTX built for MUTUAL auth (both ends
// present + verify a cert, fail-closed). The credential is the structural seam:
// it is injected by VALUE into the transport ctor (never the Policy, never a
// global). It carries the injected verify policy and stitches itself into the
// SSL_CTX ex_data slot so the C-linkage verify callback resolves the policy from
// inside the OpenSSL handshake; the stitch is re-applied on move so the slot
// always points at the live credential. The policy is REQUIRED — a credential
// built without one fails closed (the callback rejects).
class tls_credential
{
public:
    tls_credential() = default;

    // Adopt a fully-built SSL_CTX (verify callback installed, cert/key checked)
    // and the verify policy it must carry. Stitches itself into ex_data. The
    // policy is shared so every channel that shares this SSL_CTX sees the same
    // verify decision. Prefer load_credential() to mint one from disk paths.
    tls_credential(std::unique_ptr<ssl_ctx_st, void (*)(ssl_ctx_st *)> ctx,
                   std::shared_ptr<const verify_policy> policy) noexcept;

    tls_credential(const tls_credential &) = delete;
    tls_credential &operator=(const tls_credential &) = delete;
    tls_credential(tls_credential &&) noexcept;
    tls_credential &operator=(tls_credential &&) noexcept;
    ~tls_credential();

    [[nodiscard]] bool valid() const noexcept { return m_ssl_ctx != nullptr; }

    // The owned SSL_CTX, complete-typed only in the crypto TU. A channel bumps
    // its refcount (SSL_CTX_up_ref) to back its per-channel asio::ssl::context.
    [[nodiscard]] ssl_ctx_st &ssl_ctx() const noexcept { return *m_ssl_ctx; }

    [[nodiscard]] const std::shared_ptr<const verify_policy> &policy() const noexcept { return m_policy; }

    // The OpenSSL ex_data index, allocated once (via call_once) at first use.
    [[nodiscard]] static int ex_data_index() noexcept;

private:
    void stitch() noexcept;

    std::unique_ptr<ssl_ctx_st, void (*)(ssl_ctx_st *)> m_ssl_ctx{nullptr, nullptr};
    std::shared_ptr<const verify_policy> m_policy;
};

// Mint a mutual-auth credential from a PEM cert + key on disk, pinning the given
// SPKI digests as the default verify policy. Refuses a key file looser than 0600
// (an information-disclosure boundary). Throws std::runtime_error on any load /
// build failure (a misconfigured credential must never silently fall open).
[[nodiscard]] tls_credential load_credential(const std::string &cert_path,
                                             const std::string &key_path,
                                             std::shared_ptr<const verify_policy> policy,
                                             tls_version min_version = tls_version::v1_3);

}

#endif
