#ifndef HPP_GUARD_PLEXUS_TLS_DETAIL_TLS_CONTEXT_H
#define HPP_GUARD_PLEXUS_TLS_DETAIL_TLS_CONTEXT_H

#include "plexus/tls/tls_credential.h"

#include <asio/ssl/context.hpp>

#include <openssl/ssl.h>

#include <string>
#include <stdexcept>

namespace plexus::tls::detail {

// Build a per-channel asio::ssl::context that SHARES the credential's SSL_CTX.
// asio::ssl::context's native-handle ctor takes ownership of the passed SSL_CTX*
// (its dtor calls SSL_CTX_free); SSL_CTX_up_ref bumps the refcount so the
// credential's own owning handle and asio's ssl::context dtor each decrement
// exactly once. Net: the SSL_CTX outlives every channel sharing it, and freeing
// the credential releases the original ref.
inline ::asio::ssl::context share_context(const tls_credential &cred)
{
    auto *raw = &cred.ssl_ctx();
    if(::SSL_CTX_up_ref(raw) != 1)
        throw std::runtime_error("tls_channel: SSL_CTX_up_ref failed");
    return ::asio::ssl::context(raw);
}

// Extract the bare hostname (no :port) from a "host:port" address for the SNI
// server_name extension. A missing colon yields the whole string; SNI is
// best-effort, so a malformed address simply produces a best-effort hostname.
inline std::string sni_host(const std::string &addr)
{
    auto colon = addr.rfind(':');
    return colon == std::string::npos ? addr : addr.substr(0, colon);
}

}

#endif
