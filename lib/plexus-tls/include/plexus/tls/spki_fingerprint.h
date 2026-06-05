#ifndef HPP_GUARD_PLEXUS_TLS_SPKI_FINGERPRINT_H
#define HPP_GUARD_PLEXUS_TLS_SPKI_FINGERPRINT_H

#include <array>
#include <cstddef>
#include <optional>

// Forward-declared OpenSSL leaf-cert handle: the header never sees a complete
// X509 — only the .cpp TU includes <openssl/x509.h>.
struct x509_st;

namespace plexus::tls {

// A peer's Subject-Public-Key-Info digest: the FULL 32-byte SHA-256 over the
// DER-encoded SPKI, computed via OpenSSL EVP. This is the trust anchor a pinning
// policy compares against — the full digest, never a truncated prefix (a 64-bit
// pin is too weak to anchor trust on).
using spki_digest = std::array<std::byte, 32>;

// Extract and SHA-256 the SPKI of an X509 leaf. Non-throwing: every OpenSSL
// handle is freed on every path (the verify path parses untrusted peer certs),
// and a failed extraction yields std::nullopt (the caller treats absence as
// "cannot verify" and rejects, fail-closed). The DER buffer is cleansed + freed
// before return.
std::optional<spki_digest> spki_fingerprint(const x509_st &leaf) noexcept;

}

#endif
