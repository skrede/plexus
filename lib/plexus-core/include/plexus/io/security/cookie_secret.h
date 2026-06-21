#ifndef HPP_GUARD_PLEXUS_IO_SECURITY_COOKIE_SECRET_H
#define HPP_GUARD_PLEXUS_IO_SECURITY_COOKIE_SECRET_H

#include "plexus/io/security/ct_equal.h"
#include "plexus/detail/compat.h"
#include "plexus/detail/fail_closed.h"

#include <span>
#include <array>
#include <chrono>
#include <vector>
#include <cstddef>

namespace plexus::io::security {

// The injected MAC seam: a keyed MAC (SHA-256 HMAC in the default backend) over msg ->
// out_32, computed from key. The backend installs the
// only irreducible crypto primitive here; the core owns the rotation + validate logic
// over this functor (the D-LITMUS bar — the decision needs no crypto lib). Returns
// false on any MAC failure so the caller fails the cookie closed.
using hmac_fn = plexus::detail::move_only_function<bool(std::span<const std::byte> key,
                                                        std::span<const std::byte> msg,
                                                        std::span<std::byte>       out_32)>;

// The injected entropy seam: fill `out` with cryptographically-strong bytes. Returns
// false on a degraded RNG so the caller refuses to install a zero/constant key.
using rand_fn = plexus::detail::move_only_function<bool(std::span<std::byte> out)>;

// The stateless source-address cookie secret (RFC 6347 HelloVerifyRequest). The cookie
// MAC is a keyed MAC over [peer_addr || nonce] under key: the server allocates NO full handshake
// state until the source echoes a cookie proving it owns its address, blocking spoofed-
// source state-exhaustion and amplification (the cheap pre-filter the SPKI pin cannot
// replace — the pin only rejects later, after state is allocated).
//
// Single-owner state: the HMAC key is process-random (the injected rand_fn) and never
// leaves the process, so there is no on-disk secret to manage; a restart drops all
// sessions anyway, so cross-restart cookie stability is unneeded. Two nonces (current +
// previous) give a graceful validity window across rotation — mint MACs against the
// current nonce; validate recomputes against current then previous (constant-time), so
// a cookie minted just before a rotation still verifies. No static singleton, no
// thread_local. The asio endpoint -> bytes packing stays in the backend; this binds the
// already-packed peer-addr bytes.
class cookie_secret
{
public:
    // The cookie width, set from a truncation sweep. The cookie rides inside the DTLS
    // HelloVerifyRequest with no MTU pressure (well under DTLS1_COOKIE_LENGTH, 255), so
    // truncation buys nothing on the wire; the sweep (widths 8..32 over 200k forgery
    // trials) confirmed every width >= 8 round-trips clean with zero forged matches, but
    // the full 32-byte SHA-256 digest is free here and gives the maximum MAC margin
    // (2^-256 vs 2^-64 at 8). 32 is the no-truncation cell the sweep picked.
    static constexpr std::size_t k_cookie_len = 32;

    // The nonce rotation period, set from a rotation-straddle sweep. The two-nonce
    // (current + previous) tolerance survives EXACTLY one rotation boundary (the sweep
    // verified 5000/5000 at <=1 straddled rotation, 0/5000 at 2), so the worst-case
    // cookie validity window is [P, 2P) for period P. A loopback handshake flight
    // completes in <50ms, so 60s gives a >1000x margin over the flight RTT (no spurious
    // cookie-verify failure across a mid-flight rotation) while keeping the captured-
    // cookie replay window at half the 120s alternative the sweep also exercised.
    static constexpr std::chrono::seconds k_default_rotation{60};

    // Construct fail-closed: the rand_fn fills the key + both nonces; if it returns
    // false on any fill the buffer would be left zero (an all-zero HMAC key silently
    // weakens the source-spoof cookie to a forgeable constant), so refuse to construct.
    cookie_secret(hmac_fn hmac, rand_fn rand)
            : m_hmac(std::move(hmac))
            , m_rand(std::move(rand))
    {
        if(!m_rand(m_key) || !m_rand(m_cur) || !m_rand(m_prev))
            plexus::detail::fail_closed("cookie_secret: rand_fn failed (degraded RNG)");
        m_last_rotate = std::chrono::steady_clock::now();
    }

    // Rotate the nonce when the period has elapsed: current -> previous, refill current
    // from rand_fn. Generate the fresh nonce into a temporary FIRST: on rand failure
    // RETAIN the prior good (current+previous) nonces rather than installing a zero one
    // — the validity window simply does not advance until the RNG recovers.
    void maybe_rotate(std::chrono::steady_clock::time_point now)
    {
        if(now - m_last_rotate < k_default_rotation)
            return;
        std::array<std::byte, 16> next{};
        if(!m_rand(next))
            return;
        m_prev        = m_cur;
        m_cur         = next;
        m_last_rotate = now;
    }

    // Compute the keyed MAC over [peer_addr || current_nonce] into out (k_cookie_len bytes).
    // Returns false on any hmac_fn failure (the caller fails the cookie closed).
    [[nodiscard]] bool mint(std::span<const std::byte> peer_addr, std::span<std::byte> out) const
    {
        return mac(peer_addr, m_cur, out);
    }

    // True iff `cookie` constant-time-equals the MAC against the current OR previous
    // nonce (the rotation-straddle tolerance). A length mismatch rejects immediately.
    [[nodiscard]] bool validate(std::span<const std::byte> peer_addr,
                                std::span<const std::byte> cookie) const
    {
        if(cookie.size() != k_cookie_len)
            return false;
        std::array<std::byte, k_cookie_len> expected{};
        if(mac(peer_addr, m_cur, expected) && ct_equal(expected, cookie))
            return true;
        if(mac(peer_addr, m_prev, expected) && ct_equal(expected, cookie))
            return true;
        return false;
    }

private:
    // HMAC over the packed [peer_addr || nonce]: assemble the message once, MAC it.
    [[nodiscard]] bool mac(std::span<const std::byte>       peer_addr,
                           const std::array<std::byte, 16> &nonce, std::span<std::byte> out) const
    {
        std::vector<std::byte> msg(peer_addr.begin(), peer_addr.end());
        msg.insert(msg.end(), nonce.begin(), nonce.end());
        return m_hmac(m_key, msg, out);
    }

    std::array<std::byte, 32>             m_key{}; // process-random HMAC key
    std::array<std::byte, 16>             m_cur{};
    std::array<std::byte, 16>             m_prev{};
    std::chrono::steady_clock::time_point m_last_rotate{};

    // The injected functors are invoked from the const query paths (mint/validate). The
    // fallback move_only_function (the C++20 floor without std::move_only_function) has a
    // non-const call operator, so the seam holds them mutable: invoking the MAC/RAND seam
    // is an implementation detail of a logically-const cookie query, not observable state.
    mutable hmac_fn m_hmac;
    mutable rand_fn m_rand;
};

}

#endif
