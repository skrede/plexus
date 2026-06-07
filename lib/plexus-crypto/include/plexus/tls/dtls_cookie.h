#ifndef HPP_GUARD_PLEXUS_TLS_DTLS_COOKIE_H
#define HPP_GUARD_PLEXUS_TLS_DTLS_COOKIE_H

#include "plexus/io/security/cookie_secret.h"

#include <cstddef>

// Forward-declared OpenSSL handle: the cookie callbacks take an SSL* by opaque
// pointer; only the .cpp includes <openssl/ssl.h>, so OpenSSL stays out of every
// consumer translation unit (the same seam split tls_credential.h uses).
struct ssl_st;

namespace plexus::tls {

// Build the node's source-address cookie secret over the irreducible OpenSSL
// primitives: the rotation + current/previous window + constant-time validate live
// in the engine-agnostic core (io::security::cookie_secret); this factory injects
// ONLY the OpenSSL HMAC-SHA256 + RAND_bytes functors the core type drives. An
// mbedTLS backend would supply its own two functors to the same core type.
[[nodiscard]] io::security::cookie_secret make_cookie_secret();

// The C-linkage cookie callbacks OpenSSL drives during the HelloVerifyRequest
// round-trip. Both reach the per-instance peer-addr and the cookie secret via
// SSL_get_ex_data (NEVER thread_local — two concurrent handshakes would alias one
// slot and MAC the wrong peer, silently weakening source-spoof protection). The
// channel publishes its peer-addr + cookie-secret into the SSL ex_data slots
// before driving the handshake. Each delegates rotation + mint/validate to the core
// cookie_secret.
extern "C" int dtls_cookie_generate_cb(ssl_st *ssl, unsigned char *cookie, unsigned int *len);
extern "C" int dtls_cookie_verify_cb(ssl_st *ssl, const unsigned char *cookie, unsigned int len);

// The ex_data slot indices the channel publishes into and the cookie callbacks
// read from: the cookie-secret pointer and the peer-addr block. Allocated once.
[[nodiscard]] int dtls_cookie_secret_ex_index() noexcept;
[[nodiscard]] int dtls_peer_addr_ex_index() noexcept;

// The ex_data slot the channel publishes its m_peer_facts address into, and the
// verify callback writes the verify-time depth-0 leaf facts back through: the ONE
// cert extraction per handshake is stashed here as a plain value (never an X509*),
// so the later completion edge derives the peer identity without a second SPKI
// digest. Allocated once.
[[nodiscard]] int dtls_peer_facts_ex_index() noexcept;

// The C-linkage ALPN select callback (server side): selects "plexus/1" if the
// client offered it, else returns SSL_TLSEXT_ERR_ALERT_FATAL to FAIL the handshake
// on no overlap — the fail-closed in-handshake version gate (R-2). The client
// offers "plexus/1" via SSL_CTX_set_alpn_protos in the credential builder.
extern "C" int plexus_alpn_select(ssl_st *ssl, const unsigned char **out, unsigned char *outlen,
                                  const unsigned char *in, unsigned int inlen, void *arg);

}

#endif
