#ifndef HPP_GUARD_PLEXUS_TLS_DTLS_COOKIE_H
#define HPP_GUARD_PLEXUS_TLS_DTLS_COOKIE_H

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>

// Forward-declared OpenSSL handle: the cookie callbacks take an SSL* by opaque
// pointer; only the .cpp includes <openssl/ssl.h>, so OpenSSL stays out of every
// consumer translation unit (the same seam split tls_credential.h uses).
struct ssl_st;

namespace plexus::tls {

// The stateless DTLS cookie secret (RFC 6347 HelloVerifyRequest). The cookie MAC
// is HMAC(SHA-256, key, peer_addr || nonce): the server allocates NO full
// handshake state until the source echoes a cookie proving it owns its address,
// blocking spoofed-source state-exhaustion and amplification (the cheap pre-filter
// the SPKI pin cannot replace — the pin only rejects later, after state is
// allocated).
//
// Single-owner state: the HMAC key is process-random (RAND_bytes) and never leaves
// the process, so there is no on-disk secret to manage; a restart drops all
// sessions anyway, so cross-restart cookie stability is unneeded. Two nonces
// (current + previous) give a graceful validity window across rotation — generate
// MACs against the current nonce; verify recomputes against current then previous
// (constant-time), so a cookie minted just before a rotation still verifies. No
// static singleton, no thread_local.
class dtls_cookie_state
{
public:
    // The fixed cookie width. Truncating the HMAC to a fixed length keeps the
    // cookie well under DTLS1_COOKIE_LENGTH (255) while preserving ample MAC
    // strength (a 32-byte SHA-256 HMAC truncated to 32 bytes is the full digest).
    static constexpr std::size_t k_cookie_len = 32;

    // The nonce rotation period: an empirically-set knob (a dedicated sweep refines
    // it later). 60s is the starting cell — long enough that a cookie minted at the
    // start of a handshake flight is still valid when the cookie'd ClientHello
    // returns, short enough to bound a captured-cookie replay window.
    static constexpr std::chrono::seconds k_default_rotation{60};

    dtls_cookie_state();

    // Rotate the nonce when the period has elapsed: current -> previous, refill
    // current from RAND_bytes. Called before each generate/verify so the validity
    // window advances with wall time without a background timer.
    void maybe_rotate(std::chrono::steady_clock::time_point now);

    // Compute HMAC(SHA-256, key, peer_addr || nonce) truncated to k_cookie_len.
    // `current` selects the current vs previous nonce. Returns false on any HMAC
    // failure (the caller fails the cookie closed).
    bool compute(const unsigned char *peer_addr, std::size_t addr_len,
                 bool current, unsigned char *out) const;

private:
    std::array<unsigned char, 32> m_key{};        // process-random HMAC key
    std::array<unsigned char, 16> m_nonce_cur{};
    std::array<unsigned char, 16> m_nonce_prev{};
    std::chrono::steady_clock::time_point m_last_rotate{};
};

// The C-linkage cookie callbacks OpenSSL drives during the HelloVerifyRequest
// round-trip. Both reach the per-instance peer-addr and the cookie-state via
// SSL_get_ex_data (NEVER thread_local — two concurrent handshakes would alias one
// slot and MAC the wrong peer, silently weakening source-spoof protection). The
// channel publishes its peer-addr + cookie-state into the SSL ex_data slots
// before driving the handshake.
extern "C" int dtls_cookie_generate_cb(ssl_st *ssl, unsigned char *cookie, unsigned int *len);
extern "C" int dtls_cookie_verify_cb(ssl_st *ssl, const unsigned char *cookie, unsigned int len);

// The ex_data slot indices the channel publishes into and the cookie callbacks
// read from: the cookie-state pointer and the peer-addr block. Allocated once.
[[nodiscard]] int dtls_cookie_state_ex_index() noexcept;
[[nodiscard]] int dtls_peer_addr_ex_index() noexcept;

// The C-linkage ALPN select callback (server side): selects "plexus/1" if the
// client offered it, else returns SSL_TLSEXT_ERR_ALERT_FATAL to FAIL the handshake
// on no overlap — the fail-closed in-handshake version gate (R-2). The client
// offers "plexus/1" via SSL_CTX_set_alpn_protos in the credential builder.
extern "C" int plexus_alpn_select(ssl_st *ssl, const unsigned char **out, unsigned char *outlen,
                                  const unsigned char *in, unsigned int inlen, void *arg);

}

#endif
