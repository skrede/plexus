#include "plexus/tls/tls_credential.h"
#include "plexus/tls/verify_policy.h"
#include "plexus/tls/spki_fingerprint.h"

#include <openssl/ssl.h>
#include <openssl/bio.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/evp.h>
#include <openssl/crypto.h>

#include <span>
#include <mutex>
#include <utility>
#include <stdexcept>
#include <filesystem>

namespace plexus::tls {

namespace {

// Function-pointer-deleter handle aliases for the OpenSSL types consumed during
// loading + verification. Each throw/return path frees its handle via the local
// unique_ptr's dtor on unwind.
using bio_ptr      = std::unique_ptr<BIO, void (*)(BIO *)>;
using x509_ptr     = std::unique_ptr<X509, void (*)(X509 *)>;
using evp_key_ptr  = std::unique_ptr<EVP_PKEY, void (*)(EVP_PKEY *)>;
using x509_pub_ptr = std::unique_ptr<X509_PUBKEY, void (*)(X509_PUBKEY *)>;
using ssl_ctx_ptr  = std::unique_ptr<ssl_ctx_st, void (*)(ssl_ctx_st *)>;

void free_bio(BIO *b)                 { if(b) BIO_free(b); }
void free_x509(X509 *x)               { if(x) X509_free(x); }
void free_evp_pkey(EVP_PKEY *k)       { if(k) EVP_PKEY_free(k); }
void free_x509_pubkey(X509_PUBKEY *p) { if(p) X509_PUBKEY_free(p); }
void free_ssl_ctx(ssl_ctx_st *c)      { if(c) SSL_CTX_free(c); }

// SHA-256 the DER-encoded SPKI of an X509 leaf into the full 32-byte digest.
// Non-throwing; frees every handle on every path; cleanses + frees the DER.
std::optional<spki_digest> digest_of(X509 *leaf) noexcept
{
    evp_key_ptr pubkey(X509_get_pubkey(leaf), &free_evp_pkey);
    if(!pubkey)
        return std::nullopt;

    X509_PUBKEY *raw = nullptr;
    if(X509_PUBKEY_set(&raw, pubkey.get()) != 1 || !raw)
        return std::nullopt;
    x509_pub_ptr pub(raw, &free_x509_pubkey);

    unsigned char *der = nullptr;
    const int der_len = i2d_X509_PUBKEY(pub.get(), &der);
    if(der_len <= 0 || !der)
        return std::nullopt;

    spki_digest out{};
    unsigned int dlen = 0;
    const int ok = EVP_Digest(der, static_cast<std::size_t>(der_len),
                              reinterpret_cast<unsigned char *>(out.data()), &dlen,
                              EVP_sha256(), nullptr);
    OPENSSL_cleanse(der, static_cast<std::size_t>(der_len));
    OPENSSL_free(der);
    if(ok != 1 || dlen != out.size())
        return std::nullopt;
    return out;
}

// Parse a DER-encoded leaf back into an X509 (the verify contract is pure bytes),
// SHA-256 its SPKI, and free the handle. Non-throwing.
std::optional<spki_digest> digest_of_der(std::span<const std::byte> leaf_der) noexcept
{
    const unsigned char *p = reinterpret_cast<const unsigned char *>(leaf_der.data());
    x509_ptr leaf(d2i_X509(nullptr, &p, static_cast<long>(leaf_der.size())), &free_x509);
    if(!leaf)
        return std::nullopt;
    return digest_of(leaf.get());
}

// The one-shot OpenSSL ex_data index. call_once (a function, never a singleton
// object) allocates a process-stable SSL_CTX-side slot; the no-op free_func is
// null because the slot holds a NON-owning credential pointer (the credential
// owns the SSL_CTX, not the reverse).
int allocate_ex_data_index() noexcept
{
    static std::once_flag once;
    static int index = -1;
    std::call_once(once, [] {
        index = SSL_CTX_get_ex_new_index(0, nullptr, nullptr, nullptr, nullptr);
    });
    return index;
}

// The C-linkage verify callback. Walks X509_STORE_CTX -> SSL* -> SSL_CTX* ->
// ex_data -> credential -> verify_policy and returns the policy's accept/reject.
// Fail-closed at every missing link: no SSL / no credential / no policy / no
// peer cert / DER-serialize failure all REJECT. Non-throwing; frees the DER.
extern "C" int plexus_tls_verify_callback(int preverify_ok, X509_STORE_CTX *ctx)
{
    // SPKI pinning anchors trust at the leaf (depth 0). At depth > 0 we are not
    // the trust anchor, so preserve preverify_ok and let OpenSSL's own chain
    // logic own the intermediates — otherwise a legitimately-pinned peer that
    // presents a leaf+intermediate chain is rejected at depth 1.
    if(X509_STORE_CTX_get_error_depth(ctx) != 0)
        return preverify_ok;

    const int ssl_idx = SSL_get_ex_data_X509_STORE_CTX_idx();
    auto *ssl = static_cast<SSL *>(X509_STORE_CTX_get_ex_data(ctx, ssl_idx));
    auto reject = [ctx] {
        X509_STORE_CTX_set_error(ctx, X509_V_ERR_APPLICATION_VERIFICATION);
        return 0;
    };
    if(!ssl)
        return reject();

    SSL_CTX *sslctx = SSL_get_SSL_CTX(ssl);
    if(!sslctx)
        return reject();

    auto *cred = static_cast<const tls_credential *>(
        SSL_CTX_get_ex_data(sslctx, tls_credential::ex_data_index()));
    if(!cred || !cred->policy())
        return reject();

    X509 *peer = X509_STORE_CTX_get_current_cert(ctx);
    if(!peer)
        return reject();

    unsigned char *der = nullptr;
    const int der_len = i2d_X509(peer, &der);
    if(der_len <= 0 || !der)
        return reject();
    const bool ok = cred->policy()->verify(
        std::span<const std::byte>(reinterpret_cast<const std::byte *>(der),
                                   static_cast<std::size_t>(der_len)));
    OPENSSL_free(der);
    if(!ok)
    {
        X509_STORE_CTX_set_error(ctx, X509_V_ERR_CERT_REJECTED);
        return 0;
    }
    return 1;
}

[[noreturn]] void fail(const std::string &msg) { throw std::runtime_error(msg); }

// Refuse a key file looser than 0600 (an information-disclosure boundary). On
// Windows the group/other bits do not surface and this is a structural pass.
void check_key_perms(const std::string &key_path)
{
    std::error_code ec;
    auto status = std::filesystem::status(key_path, ec);
    if(ec)
        fail("tls key: stat failed: " + key_path);
    using fp = std::filesystem::perms;
    constexpr fp disallowed = fp::group_read | fp::group_write | fp::group_exec
                            | fp::others_read | fp::others_write | fp::others_exec;
    if((status.permissions() & disallowed) != fp::none)
        fail("tls key: permissions too open (must be 0600 or stricter): " + key_path);
}

x509_ptr load_cert(const std::string &path)
{
    bio_ptr bio(BIO_new_file(path.c_str(), "r"), &free_bio);
    if(!bio)
        fail("tls cert: open failed: " + path);
    x509_ptr cert(PEM_read_bio_X509(bio.get(), nullptr, nullptr, nullptr), &free_x509);
    if(!cert)
        fail("tls cert: PEM parse failed: " + path);
    return cert;
}

evp_key_ptr load_key(const std::string &path)
{
    bio_ptr bio(BIO_new_file(path.c_str(), "r"), &free_bio);
    if(!bio)
        fail("tls key: open failed: " + path);
    evp_key_ptr key(PEM_read_bio_PrivateKey(bio.get(), nullptr, nullptr, nullptr), &free_evp_pkey);
    if(!key)
        fail("tls key: PEM parse failed: " + path);
    return key;
}

// Build the mutual-auth SSL_CTX: min-version floor, cert+key install + match
// self-test, and SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT so OpenSSL
// structurally demands both ends present a cert. The verify callback is the
// plexus adapter that consults the injected policy via ex_data.
ssl_ctx_ptr build_ctx(X509 *cert, EVP_PKEY *key, tls_version min_version)
{
    ssl_ctx_ptr ctx(SSL_CTX_new(TLS_method()), &free_ssl_ctx);
    if(!ctx)
        fail("tls: SSL_CTX_new failed");
    const int floor = min_version == tls_version::v1_3 ? TLS1_3_VERSION : TLS1_2_VERSION;
    if(SSL_CTX_set_min_proto_version(ctx.get(), floor) != 1)
        fail("tls: SSL_CTX_set_min_proto_version failed");
    if(SSL_CTX_use_certificate(ctx.get(), cert) != 1)
        fail("tls cert: SSL_CTX_use_certificate failed");
    if(SSL_CTX_use_PrivateKey(ctx.get(), key) != 1)
        fail("tls: cert/key mismatch (SSL_CTX_use_PrivateKey failed)");
    if(SSL_CTX_check_private_key(ctx.get()) != 1)
        fail("tls: cert/key mismatch (SSL_CTX_check_private_key failed)");
    SSL_CTX_set_verify(ctx.get(),
                       SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT,
                       &plexus_tls_verify_callback);
    return ctx;
}

}

std::optional<spki_digest> spki_fingerprint(const x509_st &leaf) noexcept
{
    return digest_of(const_cast<X509 *>(&leaf));
}

bool spki_pin_policy::verify(std::span<const std::byte> leaf_der) const noexcept
{
    const auto peer = digest_of_der(leaf_der);
    if(!peer)
        return false;
    for(const auto &pin : m_pinned)
        if(pin == *peer)
            return true;
    return false;
}

int tls_credential::ex_data_index() noexcept { return allocate_ex_data_index(); }

void tls_credential::stitch() noexcept
{
    if(m_ssl_ctx)
        SSL_CTX_set_ex_data(m_ssl_ctx.get(), ex_data_index(), this);
}

tls_credential::tls_credential(std::unique_ptr<ssl_ctx_st, void (*)(ssl_ctx_st *)> ctx,
                               std::shared_ptr<const verify_policy> policy) noexcept
    : m_ssl_ctx(std::move(ctx))
    , m_policy(std::move(policy))
{
    stitch();
}

tls_credential::tls_credential(tls_credential &&other) noexcept
    : m_ssl_ctx(std::move(other.m_ssl_ctx))
    , m_policy(std::move(other.m_policy))
{
    stitch();   // re-point the ex_data slot at the live (this) address
}

tls_credential &tls_credential::operator=(tls_credential &&other) noexcept
{
    if(this != &other)
    {
        m_ssl_ctx = std::move(other.m_ssl_ctx);
        m_policy = std::move(other.m_policy);
        stitch();
    }
    return *this;
}

tls_credential::~tls_credential() = default;

tls_credential load_credential(const std::string &cert_path,
                               const std::string &key_path,
                               std::shared_ptr<const verify_policy> policy,
                               tls_version min_version)
{
    if(!policy)
        fail("tls: a credential requires a verify policy (no fail-open default)");
    if(!std::filesystem::exists(cert_path))
        fail("tls cert: file not found: " + cert_path);
    if(!std::filesystem::exists(key_path))
        fail("tls key: file not found: " + key_path);
    check_key_perms(key_path);

    x509_ptr cert = load_cert(cert_path);
    evp_key_ptr key = load_key(key_path);
    ssl_ctx_ptr ctx = build_ctx(cert.get(), key.get(), min_version);

    std::unique_ptr<ssl_ctx_st, void (*)(ssl_ctx_st *)> owned(ctx.release(), &free_ssl_ctx);
    return tls_credential{std::move(owned), std::move(policy)};
}

}
