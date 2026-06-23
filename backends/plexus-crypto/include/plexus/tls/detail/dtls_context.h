#ifndef HPP_GUARD_PLEXUS_TLS_DETAIL_DTLS_CONTEXT_H
#define HPP_GUARD_PLEXUS_TLS_DETAIL_DTLS_CONTEXT_H

#include "plexus/tls/tls_credential.h"

#include <memory>
#include <stdexcept>

// Forward-declared OpenSSL handle: this header never sees a complete SSL_CTX —
// the type is opaque here and only the DTLS .cpp includes <openssl/ssl.h>, so
// OpenSSL stays out of every consumer's translation unit (the same seam split
// tls_credential.h uses). SSL_CTX_up_ref is declared in the .cpp where the
// complete type is visible.
struct ssl_ctx_st;

namespace plexus::tls::detail {

// An owning, refcounted handle to a shared DTLS SSL_CTX: a fn-ptr-deleter
// unique_ptr that calls SSL_CTX_free on destruction (the deleter is supplied at
// construction from the .cpp, where the complete type and SSL_CTX_free are
// visible). Mirrors the TLS share_context up_ref accounting, but there is no
// asio::ssl::context for a DTLS method — DTLS calls SSL_new(ctx) directly — so the
// share helper hands back the refcounted SSL_CTX* itself rather than an
// asio::ssl::context wrapper.
using shared_ssl_ctx = std::unique_ptr<ssl_ctx_st, void (*)(ssl_ctx_st *)>;

// Bump the credential's SSL_CTX refcount once and return an owning handle: the
// credential's own owning handle and this returned handle each decrement exactly
// once, so the SSL_CTX outlives every channel sharing it and freeing the
// credential releases the original ref. Throws if the credential carries no
// SSL_CTX (default-constructed) or the up_ref fails — a misbuilt context never
// silently yields a null handle. Defined in the DTLS .cpp (complete-type only).
//
// Named share_dtls_context (not share_context) because the TLS share_context in
// this same namespace returns an asio::ssl::context for the same parameter list —
// two functions differing only in return type are ill-formed, so the DTLS variant
// that hands back a raw refcounted SSL_CTX* carries a distinct name.
[[nodiscard]] shared_ssl_ctx share_dtls_context(const tls_credential &cred);

}

#endif
