#ifndef HPP_GUARD_PLEXUS_IO_SECURITY_CERT_FACTS_H
#define HPP_GUARD_PLEXUS_IO_SECURITY_CERT_FACTS_H

#include "plexus/node_id.h"

#include <array>
#include <chrono>
#include <string>
#include <vector>
#include <cstddef>

namespace plexus::io::security {

// The verified-peer-certificate facts the verify decision runs over: a pure VALUE
// struct the backend fills ONCE per peer cert (the only place that touches a TLS-
// engine cert handle) and the core only READS. The core never sees an engine handle
// — the seam is a flat value type, so the accept/reject decision, the identity
// registry key, and the validity reasoning all live in the header-only core with
// zero engine dependency (an mbedTLS backend fills the same struct from its own
// cert parser).
//
// The FULL field list: the trust anchor (spki_sha256), the human identity (subject
// + san), the validity window (not_before / not_after), the chain depth, and the
// engine's own preverify verdict. A decision policy may consult any field; the
// identity stable subset (spki_sha256 + subject + san) is the documented narrow
// contract a registry key derives from.
struct cert_facts
{
    // The peer's full 32-byte SHA-256 SPKI digest — the trust anchor a pinning
    // policy compares against (the full digest, never a truncated prefix). The
    // first 16 bytes also derive the stable node_id (see to_node_id below).
    std::array<std::byte, 32> spki_sha256{};

    // The cert subject (the CN, or the full one-line subject DN if there is no CN).
    std::string subject;

    // The Subject-Alternative-Name entries (DNS / IP / URI), in cert order.
    std::vector<std::string> san;

    // The certificate validity window.
    std::chrono::system_clock::time_point not_before{};
    std::chrono::system_clock::time_point not_after{};

    // The depth of this cert in the presented chain (0 = leaf).
    int chain_depth{0};

    // The engine's own chain-verification verdict for this cert (a policy MAY layer
    // its own decision on top, but absence must never be trusted — fail-closed).
    bool preverify_ok{false};
};

// Derive the stable 16-byte node_id from the facts: the first 16 bytes of the full
// 32-byte SHA-256 SPKI digest (node_id is a 128-bit value). The full pin remains
// the trust anchor in the verify decision; this is only the registry key — no second
// SPKI extraction, the truncation reads the already-computed digest.
[[nodiscard]] inline plexus::node_id to_node_id(const cert_facts &facts) noexcept
{
    plexus::node_id id{};
    for(std::size_t i = 0; i < id.size(); ++i)
        id[i] = facts.spki_sha256[i];
    return id;
}

}

#endif
