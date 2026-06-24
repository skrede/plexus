#include "plexus/tls/tls_credential.h"
#include "plexus/tls/spki_fingerprint.h"
#include "plexus/tls/dtls_cookie.h"

#include "plexus/io/security/cert_facts.h"

#include <openssl/ssl.h>
#include <openssl/bio.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>
#include <openssl/evp.h>
#include <openssl/crypto.h>

#include <array>
#include <ctime>
#include <mutex>
#include <chrono>
#include <string>
#include <vector>
#include <utility>
#include <optional>
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

void free_bio(BIO *b)
{
    if(b)
        BIO_free(b);
}
void free_x509(X509 *x)
{
    if(x)
        X509_free(x);
}
void free_evp_pkey(EVP_PKEY *k)
{
    if(k)
        EVP_PKEY_free(k);
}
void free_x509_pubkey(X509_PUBKEY *p)
{
    if(p)
        X509_PUBKEY_free(p);
}
void free_ssl_ctx(ssl_ctx_st *c)
{
    if(c)
        SSL_CTX_free(c);
}

// SHA-256 the DER-encoded SPKI of an X509 leaf into the full 32-byte digest (the
// core cert_facts::spki_sha256 field type). Non-throwing; frees every handle on
// every path; cleanses + frees the DER.
std::optional<std::array<std::byte, 32>> digest_of(X509 *leaf) noexcept
{
    evp_key_ptr pubkey(X509_get_pubkey(leaf), &free_evp_pkey);
    if(!pubkey)
        return std::nullopt;

    X509_PUBKEY *raw = nullptr;
    if(X509_PUBKEY_set(&raw, pubkey.get()) != 1 || !raw)
        return std::nullopt;
    x509_pub_ptr pub(raw, &free_x509_pubkey);

    unsigned char *der = nullptr;
    const int der_len  = i2d_X509_PUBKEY(pub.get(), &der);
    if(der_len <= 0 || !der)
        return std::nullopt;

    std::array<std::byte, 32> out{};
    unsigned int dlen = 0;
    const int ok      = EVP_Digest(der, static_cast<std::size_t>(der_len), reinterpret_cast<unsigned char *>(out.data()), &dlen, EVP_sha256(), nullptr);
    OPENSSL_cleanse(der, static_cast<std::size_t>(der_len));
    OPENSSL_free(der);
    if(ok != 1 || dlen != out.size())
        return std::nullopt;
    return out;
}

// The leaf subject: the CN, or the full one-line subject DN if there is no CN.
std::string subject_of(X509 *leaf)
{
    X509_NAME *name = X509_get_subject_name(leaf);
    if(!name)
        return {};
    char cn[256];
    const int n = X509_NAME_get_text_by_NID(name, NID_commonName, cn, sizeof(cn));
    if(n > 0)
        return std::string(cn, static_cast<std::size_t>(n));
    char *line      = X509_NAME_oneline(name, nullptr, 0);
    std::string out = line ? line : "";
    OPENSSL_free(line);
    return out;
}

// The subjectAltName DNS / IP / URI entries, in cert order. Frees the GENERAL_NAMES
// stack on every path. Absence yields an empty vector (no SAN extension).
std::vector<std::string> san_of(X509 *leaf)
{
    std::vector<std::string> out;
    auto *names = static_cast<GENERAL_NAMES *>(X509_get_ext_d2i(leaf, NID_subject_alt_name, nullptr, nullptr));
    if(!names)
        return out;
    const int count = sk_GENERAL_NAME_num(names);
    for(int i = 0; i < count; ++i)
    {
        const GENERAL_NAME *gn = sk_GENERAL_NAME_value(names, i);
        if(!gn)
            continue;
        if(gn->type == GEN_DNS || gn->type == GEN_URI)
        {
            const unsigned char *data = ASN1_STRING_get0_data(gn->d.ia5);
            const int len             = ASN1_STRING_length(gn->d.ia5);
            if(data && len > 0)
                out.emplace_back(reinterpret_cast<const char *>(data), static_cast<std::size_t>(len));
        }
    }
    GENERAL_NAMES_free(names);
    return out;
}

// Convert an ASN1_TIME to a system_clock time_point (epoch on any parse failure —
// a policy that reasons about validity reads not_before/not_after, fail-closed).
std::chrono::system_clock::time_point time_of(const ASN1_TIME *t)
{
    if(!t)
        return {};
    std::tm tm{};
    if(ASN1_TIME_to_tm(t, &tm) != 1)
        return {};
    const std::time_t secs = timegm(&tm);
    return std::chrono::system_clock::from_time_t(secs);
}

// Extract the full cert_facts field set from a peer leaf into the core VALUE struct
// (never an X509* crosses the seam). Non-throwing; fail-closed: a missing SPKI
// digest yields false (the caller rejects). The SPKI digest reuses digest_of (the
// ONE SPKI extraction); subject / SAN / validity / depth / preverify_ok fill the
// rest. The first 16 bytes of spki_sha256 also derive the peer node_id at the
// completion edge (no second digest).
bool extract_cert_facts(X509 *peer, int depth, bool preverify_ok, io::security::cert_facts &facts) noexcept
{
    const auto digest = digest_of(peer);
    if(!digest)
        return false;
    facts.spki_sha256  = *digest;
    facts.subject      = subject_of(peer);
    facts.san          = san_of(peer);
    facts.not_before   = time_of(X509_get0_notBefore(peer));
    facts.not_after    = time_of(X509_get0_notAfter(peer));
    facts.chain_depth  = depth;
    facts.preverify_ok = preverify_ok;
    return true;
}

// The one-shot OpenSSL ex_data index. call_once (a function, never a singleton
// object) allocates a process-stable SSL_CTX-side slot; the no-op free_func is
// null because the slot holds a NON-owning credential pointer (the credential
// owns the SSL_CTX, not the reverse).
int allocate_ex_data_index() noexcept
{
    static std::once_flag once;
    static int index = -1;
    std::call_once(once, [] { index = SSL_CTX_get_ex_new_index(0, nullptr, nullptr, nullptr, nullptr); });
    return index;
}

// The C-linkage verify callback. Walks X509_STORE_CTX -> SSL* -> SSL_CTX* ->
// ex_data -> credential -> verify_policy, extracts a cert_facts VALUE from the leaf
// ONCE, and returns the core decision's accept/reject. Core never sees an X509* and
// never parses DER. Fail-closed at every missing link: no SSL / no credential / no
// policy / no peer cert / extraction failure all REJECT.
// NOLINTNEXTLINE(readability-function-size)
extern "C" int plexus_tls_verify_callback(int preverify_ok, X509_STORE_CTX *ctx)
{
    // SPKI pinning anchors trust at the leaf (depth 0). At depth > 0 we are not
    // the trust anchor, so preserve preverify_ok and let OpenSSL's own chain
    // logic own the intermediates — otherwise a legitimately-pinned peer that
    // presents a leaf+intermediate chain is rejected at depth 1.
    if(X509_STORE_CTX_get_error_depth(ctx) != 0)
        return preverify_ok;

    const int ssl_idx = SSL_get_ex_data_X509_STORE_CTX_idx();
    auto *ssl         = static_cast<SSL *>(X509_STORE_CTX_get_ex_data(ctx, ssl_idx));
    auto reject       = [ctx]
    {
        X509_STORE_CTX_set_error(ctx, X509_V_ERR_APPLICATION_VERIFICATION);
        return 0;
    };
    if(!ssl)
        return reject();

    SSL_CTX *sslctx = SSL_get_SSL_CTX(ssl);
    if(!sslctx)
        return reject();

    auto *cred = static_cast<const tls_credential *>(SSL_CTX_get_ex_data(sslctx, tls_credential::ex_data_index()));
    if(!cred || !cred->policy())
        return reject();

    X509 *peer = X509_STORE_CTX_get_current_cert(ctx);
    if(!peer)
        return reject();

    io::security::cert_facts facts;
    if(!extract_cert_facts(peer, 0, preverify_ok != 0, facts))
        return reject();
    if(!cred->policy()->decide(facts))
    {
        X509_STORE_CTX_set_error(ctx, X509_V_ERR_CERT_REJECTED);
        return 0;
    }
    // Stash the verify-time depth-0 leaf facts onto the channel via the SAME
    // per-instance SSL ex_data stitch the cookie callbacks use (never thread_local):
    // the channel published its m_peer_facts address before driving the handshake,
    // so the completion edge derives the peer identity from this ONE extraction
    // with no second SPKI digest. A null slot (the TLS path, which does not stash)
    // is simply skipped.
    if(auto *slot = static_cast<io::security::cert_facts *>(SSL_get_ex_data(ssl, dtls_peer_facts_ex_index())))
        *slot = facts;
    // The SPKI pin IS the trust anchor: a pinned leaf is fully trusted, so clear
    // any chain-building error OpenSSL set ahead of this callback (a self-signed
    // pinned leaf otherwise leaves X509_V_ERR_DEPTH_ZERO_SELF_SIGNED_CERT on the
    // store ctx, which SSL_get_verify_result would surface). Resetting to X509_V_OK
    // makes SSL_get_verify_result reflect the pin decision — the DTLS completion
    // edge gates on it as a belt-and-suspenders mutual-auth check.
    X509_STORE_CTX_set_error(ctx, X509_V_OK);
    return 1;
}

[[noreturn]] void fail(const std::string &msg)
{
    throw std::runtime_error(msg);
}

// Refuse a key file looser than 0600 (an information-disclosure boundary). On
// Windows the group/other bits do not surface and this is a structural pass.
void check_key_perms(const std::string &key_path)
{
    std::error_code ec;
    auto status = std::filesystem::status(key_path, ec);
    if(ec)
        fail("tls key: stat failed: " + key_path);
    using fp                = std::filesystem::perms;
    constexpr fp disallowed = fp::group_read | fp::group_write | fp::group_exec | fp::others_read | fp::others_write | fp::others_exec;
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
    SSL_CTX_set_verify(ctx.get(), SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, &plexus_tls_verify_callback);
    return ctx;
}

// The offered ALPN protocol_name_list: one length-prefixed name, "plexus/1" (the
// in-handshake protocol-version token; a future bump appends "plexus/2"). The
// client offers it; the server selects it via plexus_alpn_select (fail-closed on
// no overlap).
constexpr unsigned char k_alpn_protos[] = {8, 'p', 'l', 'e', 'x', 'u', 's', '/', '1'};

// Build the mutual-auth DTLS SSL_CTX: the same cert/key install + verify policy as
// the TLS path, but over DTLS_method() pinned to DTLS 1.2 (no 1.3 on this host),
// with the stateless anti-DoS cookie callbacks + SSL_OP_COOKIE_EXCHANGE and the
// in-handshake ALPN version gate. load_cert/load_key/check_key_perms and the
// verify callback are reused verbatim — only the method + DTLS-specific options
// differ.
ssl_ctx_ptr build_dtls_ctx(X509 *cert, EVP_PKEY *key)
{
    ssl_ctx_ptr ctx(SSL_CTX_new(DTLS_method()), &free_ssl_ctx);
    if(!ctx)
        fail("dtls: SSL_CTX_new failed");
    if(SSL_CTX_set_min_proto_version(ctx.get(), DTLS1_2_VERSION) != 1)
        fail("dtls: SSL_CTX_set_min_proto_version failed");
    if(SSL_CTX_set_max_proto_version(ctx.get(), DTLS1_2_VERSION) != 1)
        fail("dtls: SSL_CTX_set_max_proto_version failed");
    if(SSL_CTX_use_certificate(ctx.get(), cert) != 1)
        fail("dtls cert: SSL_CTX_use_certificate failed");
    if(SSL_CTX_use_PrivateKey(ctx.get(), key) != 1)
        fail("dtls: cert/key mismatch (SSL_CTX_use_PrivateKey failed)");
    if(SSL_CTX_check_private_key(ctx.get()) != 1)
        fail("dtls: cert/key mismatch (SSL_CTX_check_private_key failed)");
    SSL_CTX_set_verify(ctx.get(), SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, &plexus_tls_verify_callback);
    SSL_CTX_set_cookie_generate_cb(ctx.get(), &dtls_cookie_generate_cb);
    SSL_CTX_set_cookie_verify_cb(ctx.get(), &dtls_cookie_verify_cb);
    SSL_CTX_set_options(ctx.get(), SSL_OP_COOKIE_EXCHANGE);
    if(SSL_CTX_set_alpn_protos(ctx.get(), k_alpn_protos, sizeof(k_alpn_protos)) != 0)
        fail("dtls: SSL_CTX_set_alpn_protos failed");
    SSL_CTX_set_alpn_select_cb(ctx.get(), &plexus_alpn_select, nullptr);
    return ctx;
}

}

std::optional<std::array<std::byte, 32>> spki_fingerprint(const x509_st &leaf) noexcept
{
    return digest_of(const_cast<X509 *>(&leaf));
}

int tls_credential::ex_data_index() noexcept
{
    return allocate_ex_data_index();
}

void tls_credential::stitch() noexcept
{
    if(m_ssl_ctx)
        SSL_CTX_set_ex_data(m_ssl_ctx.get(), ex_data_index(), this);
}

tls_credential::tls_credential(std::unique_ptr<ssl_ctx_st, void (*)(ssl_ctx_st *)> ctx, std::shared_ptr<const io::security::verify_policy> policy) noexcept
        : m_ssl_ctx(std::move(ctx))
        , m_policy(std::move(policy))
{
    stitch();
}

tls_credential::tls_credential(tls_credential &&other) noexcept
        : m_ssl_ctx(std::move(other.m_ssl_ctx))
        , m_policy(std::move(other.m_policy))
{
    stitch(); // re-point the ex_data slot at the live (this) address
}

tls_credential &tls_credential::operator=(tls_credential &&other) noexcept
{
    if(this != &other)
    {
        m_ssl_ctx = std::move(other.m_ssl_ctx);
        m_policy  = std::move(other.m_policy);
        stitch();
    }
    return *this;
}

tls_credential::~tls_credential() = default;

tls_credential load_credential(const std::string &cert_path, const std::string &key_path, std::shared_ptr<const io::security::verify_policy> policy, tls_version min_version)
{
    if(!policy)
        fail("tls: a credential requires a verify policy (no fail-open default)");
    if(!std::filesystem::exists(cert_path))
        fail("tls cert: file not found: " + cert_path);
    if(!std::filesystem::exists(key_path))
        fail("tls key: file not found: " + key_path);
    check_key_perms(key_path);

    x509_ptr cert   = load_cert(cert_path);
    evp_key_ptr key = load_key(key_path);
    ssl_ctx_ptr ctx = build_ctx(cert.get(), key.get(), min_version);

    std::unique_ptr<ssl_ctx_st, void (*)(ssl_ctx_st *)> owned(ctx.release(), &free_ssl_ctx);
    return tls_credential{std::move(owned), std::move(policy)};
}

tls_credential load_dtls_credential(const std::string &cert_path, const std::string &key_path, std::shared_ptr<const io::security::verify_policy> policy)
{
    if(!policy)
        fail("dtls: a credential requires a verify policy (no fail-open default)");
    if(!std::filesystem::exists(cert_path))
        fail("dtls cert: file not found: " + cert_path);
    if(!std::filesystem::exists(key_path))
        fail("dtls key: file not found: " + key_path);
    check_key_perms(key_path);

    x509_ptr cert   = load_cert(cert_path);
    evp_key_ptr key = load_key(key_path);
    ssl_ctx_ptr ctx = build_dtls_ctx(cert.get(), key.get());

    std::unique_ptr<ssl_ctx_st, void (*)(ssl_ctx_st *)> owned(ctx.release(), &free_ssl_ctx);
    return tls_credential{std::move(owned), std::move(policy)};
}

}
