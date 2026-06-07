#include "plexus/tls/dtls_cookie.h"

#include <openssl/ssl.h>
#include <openssl/hmac.h>
#include <openssl/rand.h>
#include <openssl/evp.h>

#include <span>
#include <mutex>
#include <chrono>
#include <cstring>
#include <cstddef>

namespace plexus::tls {

namespace {

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

int alloc_facts_index() noexcept
{
    static std::once_flag once;
    static int index = -1;
    std::call_once(once, [] { index = SSL_get_ex_new_index(0, nullptr, nullptr, nullptr, nullptr); });
    return index;
}

// Resolve the per-instance cookie secret + peer-addr from SSL ex_data. The peer-addr
// slot holds a length-prefixed block: the first byte is the address length, the
// rest the address bytes (published by the channel before the handshake).
io::security::cookie_secret *secret_of(ssl_st *ssl)
{
    return static_cast<io::security::cookie_secret *>(SSL_get_ex_data(ssl, dtls_cookie_secret_ex_index()));
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

io::security::cookie_secret make_cookie_secret()
{
    // The two irreducible OpenSSL primitives the core cookie_secret drives: HMAC-
    // SHA256 over [key, msg] -> out (>= 32 bytes), and RAND_bytes for the key +
    // nonces. Both return false on any failure so the core fails the cookie / the
    // construction closed (a degraded RNG must never install a zero key).
    io::security::hmac_fn hmac =
        [](std::span<const std::byte> key, std::span<const std::byte> msg, std::span<std::byte> out) {
            unsigned int n = 0;
            const unsigned char *r = HMAC(EVP_sha256(),
                                          key.data(), static_cast<int>(key.size()),
                                          reinterpret_cast<const unsigned char *>(msg.data()), msg.size(),
                                          reinterpret_cast<unsigned char *>(out.data()), &n);
            return r != nullptr && n >= out.size();
        };
    io::security::rand_fn rand =
        [](std::span<std::byte> out) {
            return RAND_bytes(reinterpret_cast<unsigned char *>(out.data()),
                              static_cast<int>(out.size())) == 1;
        };
    return io::security::cookie_secret(std::move(hmac), std::move(rand));
}

int dtls_cookie_secret_ex_index() noexcept { return alloc_state_index(); }
int dtls_peer_addr_ex_index() noexcept { return alloc_addr_index(); }
int dtls_peer_facts_ex_index() noexcept { return alloc_facts_index(); }

extern "C" int dtls_cookie_generate_cb(ssl_st *ssl, unsigned char *cookie, unsigned int *len)
{
    auto *secret = secret_of(ssl);
    if(!secret)
        return 0;
    std::size_t addr_len = 0;
    const unsigned char *addr = addr_of(ssl, addr_len);
    if(!addr || addr_len == 0)
        return 0;

    secret->maybe_rotate(std::chrono::steady_clock::now());
    std::span<const std::byte> addr_bytes{reinterpret_cast<const std::byte *>(addr), addr_len};
    std::span<std::byte> out{reinterpret_cast<std::byte *>(cookie), io::security::cookie_secret::k_cookie_len};
    if(!secret->mint(addr_bytes, out))
        return 0;
    *len = static_cast<unsigned int>(io::security::cookie_secret::k_cookie_len);
    return 1;
}

extern "C" int dtls_cookie_verify_cb(ssl_st *ssl, const unsigned char *cookie, unsigned int len)
{
    auto *secret = secret_of(ssl);
    if(!secret)
        return 0;
    std::size_t addr_len = 0;
    const unsigned char *addr = addr_of(ssl, addr_len);
    if(!addr || addr_len == 0)
        return 0;

    secret->maybe_rotate(std::chrono::steady_clock::now());
    std::span<const std::byte> addr_bytes{reinterpret_cast<const std::byte *>(addr), addr_len};
    std::span<const std::byte> cookie_bytes{reinterpret_cast<const std::byte *>(cookie), len};
    return secret->validate(addr_bytes, cookie_bytes) ? 1 : 0;
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
