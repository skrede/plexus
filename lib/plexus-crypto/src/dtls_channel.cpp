#include "plexus/tls/dtls_channel.h"

#include <openssl/ssl.h>
#include <openssl/bio.h>
#include <openssl/x509.h>

#include <chrono>
#include <cstring>
#include <algorithm>
#include <stdexcept>

namespace plexus::tls {

namespace {

void free_bio(BIO *b) { if(b) BIO_free(b); }

int to_int(std::size_t v) noexcept { return static_cast<int>(v); }

// Pack a udp endpoint into the cookie-bound peer-addr block: a length-prefixed
// [addr-bytes || 2-byte port] (RFC 6347 §4.2.1 binds the cookie HMAC to the source
// address). v4 packs 4 bytes, v6 packs 16; the port disambiguates a NAT-shared
// address. The first byte is the total payload length the cookie cb reads back.
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

// SHA-256 the DER-encoded SPKI of the peer leaf into a 16-byte node_id (the first
// 16 bytes of the full 32-byte SPKI digest — node_id is a 128-bit value). The full
// pin is still the trust anchor in the verify cb; this is only the registry key.
void spki_to_node_id(X509 *leaf, node_id &out)
{
    EVP_PKEY *pub = X509_get_pubkey(leaf);
    if(!pub)
        return;
    X509_PUBKEY *xpub = nullptr;
    if(X509_PUBKEY_set(&xpub, pub) == 1 && xpub)
    {
        unsigned char *der = nullptr;
        const int der_len = i2d_X509_PUBKEY(xpub, &der);
        if(der_len > 0 && der)
        {
            unsigned char digest[EVP_MAX_MD_SIZE];
            unsigned int dlen = 0;
            if(EVP_Digest(der, static_cast<std::size_t>(der_len), digest, &dlen, EVP_sha256(), nullptr) == 1
               && dlen >= out.size())
                std::memcpy(out.data(), digest, out.size());
            OPENSSL_free(der);
        }
        X509_PUBKEY_free(xpub);
    }
    EVP_PKEY_free(pub);
}

// The cert subject CN (R-OQ3: node_name == cert subject). Falls back to the full
// one-line subject DN if there is no CN entry.
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

dtls_channel::dtls_channel(::asio::io_context &io, plexus::asio::udp_server &server,
                           ::asio::ip::udp::endpoint dest, const tls_credential &cred,
                           dtls_cookie_state &cookie_state, role r, std::size_t max_payload)
    : m_io(io)
    , m_server(server)
    , m_dest(std::move(dest))
    , m_cookie_state(cookie_state)
    , m_role(r)
    , m_max_payload(max_payload)
    , m_ssl_ctx(detail::share_dtls_context(cred))
    , m_retransmit(io)
    , m_peer_addr_block(pack_peer_addr(m_dest))
{
    BIO *internal = nullptr;
    BIO *external = nullptr;
    if(::BIO_new_bio_pair(&internal, 0, &external, 0) != 1)
        throw std::runtime_error("dtls_channel: BIO_new_bio_pair failed");
    m_external_bio = external;

    m_ssl = ::SSL_new(m_ssl_ctx.get());
    if(!m_ssl)
    {
        ::BIO_free(internal);
        ::BIO_free(external);
        m_external_bio = nullptr;
        throw std::runtime_error("dtls_channel: SSL_new failed");
    }
    ::SSL_set_bio(m_ssl, internal, internal);   // SSL owns + frees the internal BIO
    ::SSL_set_mtu(m_ssl, k_dtls_mtu);

    publish_cookie_ex_data();

    if(m_role == role::server)
        ::SSL_set_accept_state(m_ssl);
    else
        ::SSL_set_connect_state(m_ssl);
}

dtls_channel::~dtls_channel()
{
    m_open = false;
    m_retransmit.cancel();                       // cancel the timer FIRST (Pitfall 6)
    if(m_ssl)
    {
        ::SSL_set_ex_data(m_ssl, dtls_peer_addr_ex_index(), nullptr);
        ::SSL_set_ex_data(m_ssl, dtls_cookie_state_ex_index(), nullptr);
        ::SSL_free(m_ssl);                       // frees the internal BIO too
        m_ssl = nullptr;
    }
    free_bio(static_cast<BIO *>(m_external_bio));
    m_external_bio = nullptr;
}

void dtls_channel::publish_cookie_ex_data()
{
    ::SSL_set_ex_data(m_ssl, dtls_cookie_state_ex_index(), &m_cookie_state);
    ::SSL_set_ex_data(m_ssl, dtls_peer_addr_ex_index(), m_peer_addr_block.data());
}

void dtls_channel::start_handshake()
{
    if(!m_open || !m_ssl)
        return;
    (void)::SSL_do_handshake(m_ssl);
    drain_outbound();
    arm_retransmit();
}

void dtls_channel::send(std::span<const std::byte> bytes)
{
    if(!m_open || !m_ssl || !m_complete_fired)
        return;
    // R-1: reject any app frame larger than the encrypted per-record budget.
    const long data_mtu = static_cast<long>(::DTLS_get_data_mtu(m_ssl));
    const std::size_t cap = data_mtu > 0
        ? std::min(m_max_payload, static_cast<std::size_t>(data_mtu))
        : m_max_payload;
    if(bytes.size() > cap)
    {
        if(m_on_error)
            m_on_error(io::io_error::message_too_large);
        return;
    }
    const int n = ::SSL_write(m_ssl, bytes.data(), to_int(bytes.size()));
    if(n <= 0)
    {
        const int err = ::SSL_get_error(m_ssl, n);
        if(err != SSL_ERROR_WANT_READ && err != SSL_ERROR_WANT_WRITE)
        {
            fail(io::io_error::broken_pipe);
            return;
        }
    }
    drain_outbound();                            // UNCONDITIONAL (Pitfall 2)
}

void dtls_channel::deliver_inbound(std::span<const std::byte> datagram)
{
    if(!m_open || !m_ssl || datagram.empty())
        return;
    auto *bio = static_cast<BIO *>(m_external_bio);
    (void)::BIO_write(bio, datagram.data(), to_int(datagram.size()));
    drain_inbound();
}

void dtls_channel::drain_outbound()
{
    auto *bio = static_cast<BIO *>(m_external_bio);
    if(!bio)
        return;
    while(true)
    {
        const int n = ::BIO_read(bio, m_drain_buf.data(), to_int(m_drain_buf.size()));
        if(n <= 0)
            break;
        m_send_scratch.assign(reinterpret_cast<const std::byte *>(m_drain_buf.data()),
                              reinterpret_cast<const std::byte *>(m_drain_buf.data()) + n);
        m_server.send_to(m_send_scratch, m_dest);
    }
}

void dtls_channel::drain_inbound()
{
    if(!m_ssl)
        return;
    while(true)
    {
        const int n = ::SSL_read(m_ssl, m_drain_buf.data(), to_int(m_drain_buf.size()));
        if(n <= 0)
        {
            const int err = ::SSL_get_error(m_ssl, n);
            if(err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE)
            {
                drain_outbound();                // mid-handshake: push the next flight
                arm_retransmit();
                break;
            }
            if(err == SSL_ERROR_ZERO_RETURN)     // clean close_notify
            {
                m_open = false;
                if(m_on_closed)
                    m_on_closed();
                break;
            }
            fail(io::io_error::broken_pipe);
            break;
        }
        post_on_data(std::span<const std::byte>{
            reinterpret_cast<const std::byte *>(m_drain_buf.data()), static_cast<std::size_t>(n)});
    }
    try_complete();
}

void dtls_channel::try_complete()
{
    if(m_complete_fired || !m_ssl || !::SSL_is_init_finished(m_ssl))
        return;
    // Fail-closed mutual-auth gate (Pitfall 4): a verified peer cert is mandatory.
    X509 *peer = ::SSL_get1_peer_certificate(m_ssl);
    const bool ok = peer && ::SSL_get_verify_result(m_ssl) == X509_V_OK;
    if(ok)
    {
        capture_peer_identity(m_ssl);
        m_complete_fired = true;
        if(peer)
            ::X509_free(peer);
        if(m_on_external_complete)
            m_on_external_complete();
        return;
    }
    if(peer)
        ::X509_free(peer);
    fail(io::io_error::connection_refused);      // fail-closed: no completion
}

void dtls_channel::capture_peer_identity(ssl_st *ssl)
{
    X509 *peer = ::SSL_get1_peer_certificate(ssl);
    if(!peer)
        return;
    spki_to_node_id(peer, m_node_id);
    m_node_name = subject_name_of(peer);
    ::X509_free(peer);
}

void dtls_channel::arm_retransmit()
{
    if(!m_ssl || !m_open)
        return;
    struct timeval tv{};
    if(::DTLSv1_get_timeout(m_ssl, &tv) != 1)
        return;
    const auto ms = std::chrono::milliseconds(
        static_cast<std::int64_t>(tv.tv_sec) * 1000 + static_cast<std::int64_t>(tv.tv_usec) / 1000);
    m_retransmit.cancel();
    m_retransmit.expires_after(ms);
    m_retransmit.async_wait([this](std::error_code ec) {
        if(ec)                                   // operation_aborted on teardown self-guards
            return;
        on_retransmit();
    });
}

void dtls_channel::on_retransmit()
{
    if(!m_ssl || !m_open)
        return;
    const int r = ::DTLSv1_handle_timeout(m_ssl);
    if(r > 0)
        drain_outbound();
    if(r >= 0)
        arm_retransmit();
}

void dtls_channel::close()
{
    if(!m_open)
        return;
    m_open = false;
    m_retransmit.cancel();
    ::asio::post(m_io, [this] { if(m_on_closed) m_on_closed(); });   // posted, never synchronous
}

void dtls_channel::fail(io::io_error e)
{
    if(!m_open)
        return;
    m_open = false;
    m_retransmit.cancel();
    if(m_on_error)
        m_on_error(e);
}

void dtls_channel::post_on_data(std::span<const std::byte> frame)
{
    auto owned = std::make_shared<std::vector<std::byte>>(frame.begin(), frame.end());
    ::asio::post(m_io, [this, owned] {
        if(m_on_data)
            m_on_data(std::span<const std::byte>{*owned});
    });
}

io::endpoint dtls_channel::remote_endpoint() const
{
    return {"dtls", m_dest.address().to_string() + ":" + std::to_string(m_dest.port())};
}

}
