#ifndef HPP_GUARD_PLEXUS_TLS_SPKI_FINGERPRINT_H
#define HPP_GUARD_PLEXUS_TLS_SPKI_FINGERPRINT_H

#include <array>
#include <cstddef>
#include <optional>

// Forward-declared OpenSSL leaf-cert handle: the header never sees a complete
// X509 — only the .cpp TU includes <openssl/x509.h>.
struct x509_st;

namespace plexus::tls {

// Extract and SHA-256 the SPKI of an X509 leaf into the FULL 32-byte digest (the
// trust anchor a pinning policy compares against — the full digest, never a
// truncated prefix). The result is exactly the core cert_facts::spki_sha256 field
// type. Non-throwing: every OpenSSL handle is freed on every path (the verify path
// parses untrusted peer certs), and a failed extraction yields std::nullopt (the
// caller treats absence as "cannot verify" and rejects, fail-closed). The DER
// buffer is cleansed + freed before return.
std::optional<std::array<std::byte, 32>> spki_fingerprint(const x509_st &leaf) noexcept;

}

#endif
