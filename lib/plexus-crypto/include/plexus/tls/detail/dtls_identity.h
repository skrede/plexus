#ifndef HPP_GUARD_PLEXUS_TLS_DETAIL_DTLS_IDENTITY_H
#define HPP_GUARD_PLEXUS_TLS_DETAIL_DTLS_IDENTITY_H

#include "plexus/node_id.h"

#include <asio/ip/udp.hpp>

#include <string>
#include <vector>

// Forward-declared OpenSSL leaf-cert handle: this header never sees a complete
// X509 — only the .cpp includes <openssl/x509.h>, so OpenSSL stays out of the
// consuming translation unit (the same seam split spki_fingerprint.h uses).
struct x509_st;

namespace plexus::tls::detail {

// The DTLS channel's identity + address-packing helpers, split from the BIO-pair
// pump (a cohesive cert-identity / cookie-addr unit). The channel publishes the
// packed peer-addr into the SSL ex_data for the cookie MAC and derives the peer
// node_id / node_name from the verified leaf at the completion edge.

// Pack a udp endpoint into the cookie-bound peer-addr block: a length-prefixed
// [addr-bytes || 2-byte port] (RFC 6347 §4.2.1 binds the cookie HMAC to the
// source address). v4 packs 4 bytes, v6 packs 16; the port disambiguates a
// NAT-shared address. The first byte is the total payload length the cookie cb
// reads back.
[[nodiscard]] std::vector<unsigned char> pack_peer_addr(const ::asio::ip::udp::endpoint &ep);

// Derive the 16-byte node_id from the peer leaf's SPKI: the first 16 bytes of the
// full 32-byte SHA-256 SPKI digest (node_id is a 128-bit value). Reuses the shared
// spki_fingerprint path — no second hand-rolled SPKI->digest copy. The full pin is
// still the trust anchor in the verify cb; this is only the registry key. Leaves
// `out` untouched on any extraction failure.
void spki_to_node_id(x509_st *leaf, node_id &out);

// The cert subject CN (R-OQ3: node_name == cert subject). Falls back to the full
// one-line subject DN if there is no CN entry.
[[nodiscard]] std::string subject_name_of(x509_st *leaf);

}

#endif
