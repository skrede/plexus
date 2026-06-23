#include "plexus/tls/detail/dtls_context.h"

#include "plexus/tls/tls_credential.h"

#include <openssl/ssl.h>

#include <stdexcept>

namespace plexus::tls::detail {

namespace {

void free_ssl_ctx(ssl_ctx_st *c)
{
    if(c)
        SSL_CTX_free(c);
}

}

shared_ssl_ctx share_dtls_context(const tls_credential &cred)
{
    if(!cred.valid())
        throw std::runtime_error("dtls_channel: credential has no SSL_CTX (default-constructed?)");
    auto *raw = &cred.ssl_ctx();
    if(SSL_CTX_up_ref(raw) != 1)
        throw std::runtime_error("dtls_channel: SSL_CTX_up_ref failed");
    return shared_ssl_ctx(raw, &free_ssl_ctx);
}

}
