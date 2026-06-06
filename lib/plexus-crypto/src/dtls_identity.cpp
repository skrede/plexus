#include "plexus/tls/detail/dtls_identity.h"

#include "plexus/tls/spki_fingerprint.h"

#include <openssl/x509.h>
#include <openssl/crypto.h>

#include <cstring>
#include <algorithm>

namespace plexus::tls::detail {

std::vector<unsigned char> pack_peer_addr(const ::asio::ip::udp::endpoint &ep)
{
    std::vector<unsigned char> block;
    const auto addr = ep.address();
    std::vector<unsigned char> raw;
    if(addr.is_v4())
    {
        auto b = addr.to_v4().to_bytes();
        raw.assign(b.begin(), b.end());
    }
    else
    {
        auto b = addr.to_v6().to_bytes();
        raw.assign(b.begin(), b.end());
    }
    const std::uint16_t port = ep.port();
    raw.push_back(static_cast<unsigned char>(port >> 8));
    raw.push_back(static_cast<unsigned char>(port & 0xff));

    block.reserve(raw.size() + 1);
    block.push_back(static_cast<unsigned char>(raw.size()));
    block.insert(block.end(), raw.begin(), raw.end());
    return block;
}

void spki_to_node_id(X509 *leaf, node_id &out)
{
    // Reuse the shared full-32-byte SPKI digest (the same EVP path tls_credential's
    // digest_of cleanses + frees) and truncate to the 128-bit node_id — no second
    // hand-rolled SPKI->digest copy.
    const auto digest = spki_fingerprint(*leaf);
    if(!digest)
        return;
    std::memcpy(out.data(), digest->data(), out.size());
}

std::string subject_name_of(X509 *leaf)
{
    X509_NAME *name = X509_get_subject_name(leaf);
    if(!name)
        return {};
    char cn[256];
    const int n = X509_NAME_get_text_by_NID(name, NID_commonName, cn, sizeof(cn));
    if(n > 0)
        return std::string(cn, static_cast<std::size_t>(n));
    char *line = X509_NAME_oneline(name, nullptr, 0);
    std::string out = line ? line : "";
    OPENSSL_free(line);
    return out;
}

}
