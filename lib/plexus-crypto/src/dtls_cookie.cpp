#include "plexus/tls/dtls_cookie.h"

#include <openssl/ssl.h>
#include <openssl/hmac.h>
#include <openssl/rand.h>
#include <openssl/evp.h>
#include <openssl/crypto.h>

#include <mutex>
#include <cstring>
#include <stdexcept>

namespace plexus::tls {

namespace {

// The peer-address block the channel publishes per-instance into SSL ex_data: the
// raw sockaddr-equivalent bytes the cookie MAC binds to (RFC 6347 §4.2.1). Stored
// by the single-owner channel; the cookie callbacks read it back NON-owningly.
constexpr std::size_t k_max_addr = 64;

int alloc_state_index() noexcept
{
    static std::once_flag once;
    static int index = -1;
    std::call_once(once, [] { index = SSL_get_ex_new_index(0, nullptr, nullptr, nullptr, nullptr); });
    return index;
}

int alloc_addr_index() noexcept
{
    static std::once_flag once;
    static int index = -1;
    std::call_once(once, [] { index = SSL_get_ex_new_index(0, nullptr, nullptr, nullptr, nullptr); });
    return index;
}

// Resolve the per-instance cookie-state + peer-addr from SSL ex_data. The peer-addr
// slot holds a length-prefixed block: the first byte is the address length, the
// rest the address bytes (published by the channel before the handshake).
dtls_cookie_state *state_of(ssl_st *ssl)
{
    return static_cast<dtls_cookie_state *>(SSL_get_ex_data(ssl, dtls_cookie_state_ex_index()));
}

const unsigned char *addr_of(ssl_st *ssl, std::size_t &len_out)
{
    auto *block = static_cast<const unsigned char *>(SSL_get_ex_data(ssl, dtls_peer_addr_ex_index()));
    if(!block)
    {
        len_out = 0;
        return nullptr;
    }
    len_out = block[0];
    return block + 1;
}

}

dtls_cookie_state::dtls_cookie_state()
{
    // Fail-closed on a degraded RNG: RAND_bytes returns <=1 leaving the buffer
    // value-initialized to zero, and an all-zero HMAC key silently weakens the
    // source-spoof cookie to a forgeable constant. Refuse to construct.
    if(RAND_bytes(m_key.data(), static_cast<int>(m_key.size())) != 1
       || RAND_bytes(m_nonce_cur.data(), static_cast<int>(m_nonce_cur.size())) != 1
       || RAND_bytes(m_nonce_prev.data(), static_cast<int>(m_nonce_prev.size())) != 1)
        throw std::runtime_error("dtls_cookie_state: RAND_bytes failed (degraded RNG)");
    m_last_rotate = std::chrono::steady_clock::now();
}

void dtls_cookie_state::maybe_rotate(std::chrono::steady_clock::time_point now)
{
    if(now - m_last_rotate < k_default_rotation)
        return;
    // Generate the fresh nonce into a temporary FIRST: on RAND_bytes failure retain
    // the prior good (current+previous) nonces rather than installing a zero nonce.
    // The validity window simply does not advance until the RNG recovers.
    std::array<unsigned char, 16> next{};
    if(RAND_bytes(next.data(), static_cast<int>(next.size())) != 1)
        return;
    m_nonce_prev = m_nonce_cur;
    m_nonce_cur = next;
    m_last_rotate = now;
}

bool dtls_cookie_state::compute(const unsigned char *peer_addr, std::size_t addr_len,
                                bool current, unsigned char *out) const
{
    const auto &nonce = current ? m_nonce_cur : m_nonce_prev;

    unsigned char msg[k_max_addr + 16];
    if(addr_len > k_max_addr)
        return false;
    std::memcpy(msg, peer_addr, addr_len);
    std::memcpy(msg + addr_len, nonce.data(), nonce.size());

    unsigned char mac[EVP_MAX_MD_SIZE];
    unsigned int mac_len = 0;
    const unsigned char *r = HMAC(EVP_sha256(), m_key.data(), static_cast<int>(m_key.size()),
                                  msg, addr_len + nonce.size(), mac, &mac_len);
    if(!r || mac_len < k_cookie_len)
        return false;
    std::memcpy(out, mac, k_cookie_len);
    return true;
}

int dtls_cookie_state_ex_index() noexcept { return alloc_state_index(); }
int dtls_peer_addr_ex_index() noexcept { return alloc_addr_index(); }

extern "C" int dtls_cookie_generate_cb(ssl_st *ssl, unsigned char *cookie, unsigned int *len)
{
    auto *state = state_of(ssl);
    if(!state)
        return 0;
    std::size_t addr_len = 0;
    const unsigned char *addr = addr_of(ssl, addr_len);
    if(!addr || addr_len == 0)
        return 0;

    state->maybe_rotate(std::chrono::steady_clock::now());
    if(!state->compute(addr, addr_len, /*current=*/true, cookie))
        return 0;
    *len = static_cast<unsigned int>(dtls_cookie_state::k_cookie_len);
    return 1;
}

extern "C" int dtls_cookie_verify_cb(ssl_st *ssl, const unsigned char *cookie, unsigned int len)
{
    auto *state = state_of(ssl);
    if(!state)
        return 0;
    std::size_t addr_len = 0;
    const unsigned char *addr = addr_of(ssl, addr_len);
    if(!addr || addr_len == 0)
        return 0;
    if(len != dtls_cookie_state::k_cookie_len)
        return 0;

    state->maybe_rotate(std::chrono::steady_clock::now());

    // Recompute against the current then the previous nonce (rotation tolerance);
    // constant-time compare so a near-miss leaks no timing about the MAC.
    unsigned char expected[dtls_cookie_state::k_cookie_len];
    if(state->compute(addr, addr_len, /*current=*/true, expected)
       && CRYPTO_memcmp(expected, cookie, len) == 0)
        return 1;
    if(state->compute(addr, addr_len, /*current=*/false, expected)
       && CRYPTO_memcmp(expected, cookie, len) == 0)
        return 1;
    return 0;
}

extern "C" int plexus_alpn_select(ssl_st *, const unsigned char **out, unsigned char *outlen,
                                  const unsigned char *in, unsigned int inlen, void *)
{
    static const unsigned char wanted[] = {'p', 'l', 'e', 'x', 'u', 's', '/', '1'};
    constexpr unsigned char wanted_len = sizeof(wanted);

    // ALPN protocol_name_list: a sequence of length-prefixed names. Scan for an
    // exact "plexus/1"; on no overlap FAIL the handshake (fail-closed version gate).
    unsigned int i = 0;
    while(i + 1 <= inlen)
    {
        const unsigned char name_len = in[i];
        if(i + 1 + name_len > inlen)
            break;
        if(name_len == wanted_len && std::memcmp(in + i + 1, wanted, wanted_len) == 0)
        {
            *out = in + i + 1;
            *outlen = name_len;
            return SSL_TLSEXT_ERR_OK;
        }
        i += 1 + name_len;
    }
    return SSL_TLSEXT_ERR_ALERT_FATAL;
}

}
