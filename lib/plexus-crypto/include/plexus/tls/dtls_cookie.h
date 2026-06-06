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
    // The cookie width, set from a truncation sweep. The cookie rides inside the
    // DTLS HelloVerifyRequest with no MTU pressure (well under DTLS1_COOKIE_LENGTH,
    // 255), so truncation buys nothing on the wire; the sweep (widths 8..32 over 200k
    // forgery trials) confirmed every width >= 8 round-trips clean with zero forged
    // matches, but the full 32-byte SHA-256 digest is free here and gives the maximum
    // MAC margin (2^-256 vs 2^-64 at 8). 32 is the no-truncation cell the sweep picked.
    static constexpr std::size_t k_cookie_len = 32;

    // The nonce rotation period, set from a rotation-straddle sweep. The two-nonce
    // (current + previous) tolerance survives EXACTLY one rotation boundary (the sweep
    // verified 5000/5000 at <=1 straddled rotation, 0/5000 at 2), so the worst-case
    // cookie validity window is [P, 2P) for period P. A loopback handshake flight
    // completes in <50ms, so 60s gives a >1000x margin over the flight RTT (no spurious
    // cookie-verify failure across a mid-flight rotation) while keeping the captured-
    // cookie replay window at half the 120s alternative the sweep also exercised.
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
