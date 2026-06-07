#ifndef HPP_GUARD_PLEXUS_TLS_DETAIL_DTLS_IDENTITY_H
#define HPP_GUARD_PLEXUS_TLS_DETAIL_DTLS_IDENTITY_H

#include <asio/ip/udp.hpp>

#include <vector>

namespace plexus::tls::detail {

// The DTLS channel's cookie-address packer. The peer-identity (node_id + node_name)
// is no longer derived here: it comes from the ONE verify-time cert_facts extraction
// (lib/plexus-crypto/src/tls_credential.cpp), stashed onto the channel via SSL
// ex_data and read at the completion edge — so there is exactly one SPKI digest per
// handshake and no X509-coupled identity extraction at the capture site.

// Pack a udp endpoint into the cookie-bound peer-addr block: a length-prefixed
// [addr-bytes || 2-byte port] (RFC 6347 §4.2.1 binds the cookie HMAC to the
// source address). v4 packs 4 bytes, v6 packs 16; the port disambiguates a
// NAT-shared address. The first byte is the total payload length the cookie cb
// reads back. This bridges asio -> bytes for the cookie; the core cookie_secret
// binds the already-packed peer-addr span.
[[nodiscard]] std::vector<unsigned char> pack_peer_addr(const ::asio::ip::udp::endpoint &ep);

}

#endif
