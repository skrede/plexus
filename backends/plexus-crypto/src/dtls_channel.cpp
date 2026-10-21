#include "plexus/tls/dtls_channel.h"

#include "plexus/tls/detail/dtls_identity.h"

#include "plexus/asio/detail/udp_channel_lifecycle.h"

#include <openssl/bio.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>

#include <chrono>
#include <cstring>
#include <utility>
#include <algorithm>
#include <stdexcept>

namespace plexus::tls {

namespace {

void free_bio(BIO *b)
{
    if(b)
        BIO_free(b);
}

int to_int(std::size_t v) noexcept
{
    return static_cast<int>(v);
}

}

// NOLINTNEXTLINE(readability-function-size)
dtls_channel::dtls_channel(::asio::io_context &io, plexus::asio::udp_server &server, ::asio::ip::udp::endpoint dest, const tls_credential &cred,
                           io::security::cookie_secret &cookie_state, role r, std::size_t max_payload, std::size_t record_mtu, std::size_t max_message_bytes,
                           std::size_t reassembly_budget, std::chrono::milliseconds reassembly_timeout)
        : m_io(io)
        , m_server(server)
        , m_dest(std::move(dest))
        , m_cookie_state(cookie_state)
        , m_role(r)
        , m_max_payload(max_payload)
        , m_record_mtu(record_mtu)
        , m_max_message_bytes(max_message_bytes)
        , m_reassembly_budget(reassembly_budget)
        , m_reassembly_timeout(reassembly_timeout)
        , m_ssl_ctx(detail::share_dtls_context(cred))
        , m_retransmit(io)
        , m_gate([this](std::span<const std::byte> bytes) { secure_send(bytes); })
        , m_peer_addr_block(detail::pack_peer_addr(m_dest))
        , m_ops()
        , m_closed()
{
    m_ops.on_settled([this] { notify_closed(); });
    // One record either direction fits one buffer: the record budget plus headroom for the 13-byte header and worst-case
    // cipher expansion, so SSL_read never discards and BIO_read never splits a record once record_mtu rises.
    constexpr std::size_t k_record_overhead = 256;
    m_drain_buf.resize(m_record_mtu + k_record_overhead);

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
    ::SSL_set_bio(m_ssl, internal, internal); // SSL owns + frees the internal BIO
    ::SSL_set_mtu(m_ssl, dtls_mtu());         // construction edge: the configured record budget

    publish_cookie_ex_data();

    if(m_role == role::server)
        ::SSL_set_accept_state(m_ssl);
    else
        ::SSL_set_connect_state(m_ssl);
}

dtls_channel::~dtls_channel()
{
    m_ops.on_settled({});
    plexus::asio::detail::udp_cancel_unhooked(m_reassembler);
    if(auto leave_demux = std::exchange(m_on_teardown_cb, nullptr))
        leave_demux();
    if(m_ssl)
    {
        ::SSL_set_ex_data(m_ssl, dtls_peer_addr_ex_index(), nullptr);
        ::SSL_set_ex_data(m_ssl, dtls_cookie_secret_ex_index(), nullptr);
        ::SSL_set_ex_data(m_ssl, dtls_peer_facts_ex_index(), nullptr);
        ::SSL_free(m_ssl); // frees the internal BIO too
        m_ssl = nullptr;
    }
    free_bio(static_cast<BIO *>(m_external_bio));
    m_external_bio = nullptr;
    m_gate.reset(); // owned block reverse-destroyed last
}

void dtls_channel::publish_cookie_ex_data()
{
    ::SSL_set_ex_data(m_ssl, dtls_cookie_secret_ex_index(), &m_cookie_state);
    ::SSL_set_ex_data(m_ssl, dtls_peer_addr_ex_index(), m_peer_addr_block.data());
    ::SSL_set_ex_data(m_ssl, dtls_peer_facts_ex_index(), &m_peer_facts);
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
    if(!m_open || !m_ssl || !m_gate.is_ready())
        return;
    // Every send rides the UDP envelope, so the FRAGMENTED bit discriminates whole from fragment and an opaque frame's
    // first byte can never alias it, as a bare in-band marker could.
    if(bytes.size() + wire::udp_envelope_overhead > record_budget())
        return send_large(bytes);
    wire::wrap_udp_into(m_frag_scratch, wire::udp_envelope_kind::best_effort, 0, bytes);
    m_gate.submit(std::span<const std::byte>{m_frag_scratch}); // ready -> pass straight to secure_send
}

std::size_t dtls_channel::record_budget() const noexcept
{
    const long data_mtu = static_cast<long>(::DTLS_get_data_mtu(m_ssl));
    return data_mtu > 0 ? std::min(m_max_payload, static_cast<std::size_t>(data_mtu)) : m_max_payload;
}

void dtls_channel::send_large(std::span<const std::byte> bytes)
{
    if(bytes.size() > m_max_message_bytes)
    {
        if(m_on_error_cb)
            m_on_error_cb(io::io_error::message_too_large);
        return;
    }
    const std::uint16_t msg_id = m_out_msg_id++;
    io::fragment_sink sink     = [this, msg_id](std::uint32_t idx, std::uint32_t cnt, std::span<const std::byte> slice)
    {
        // DTLS owns its anti-replay, so the envelope's seq field is unused.
        wire::wrap_udp_fragment_into(m_frag_scratch, wire::udp_envelope_kind::best_effort, 0, msg_id, idx, cnt, slice);
        m_gate.submit(std::span<const std::byte>{m_frag_scratch});
    };
    io::split(bytes, record_budget(), msg_id, sink);
}

void dtls_channel::secure_send(std::span<const std::byte> bytes)
{
    if(!m_open || !m_ssl)
        return;
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
    drain_outbound(); // UNCONDITIONAL
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
        // udp_server's send queue copies the datagram, so the next BIO_read may overwrite the drain buffer at once.
        m_server.send_to(std::span<const std::byte>{reinterpret_cast<const std::byte *>(m_drain_buf.data()), static_cast<std::size_t>(n)}, m_dest);
    }
}

// NOLINTNEXTLINE(readability-function-size)
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
                drain_outbound(); // mid-handshake: push the next flight
                arm_retransmit();
                break;
            }
            if(err == SSL_ERROR_ZERO_RETURN) // clean close_notify
            {
                close();
                break;
            }
            drain_outbound(); // flush any fatal alert to the peer first
            fail(io::io_error::broken_pipe);
            break;
        }
        const std::span<const std::byte> plaintext{reinterpret_cast<const std::byte *>(m_drain_buf.data()), static_cast<std::size_t>(n)};
        // A record under the envelope overhead is dropped, never indexed past.
        if(auto dec = wire::unwrap_udp(plaintext); dec)
        {
            if(dec->fragmented)
                feed_fragment(dec->frame);
            else
                post_on_data(dec->frame);
        }
    }
    try_complete();
}

// NOLINTNEXTLINE(readability-function-size)
void dtls_channel::try_complete()
{
    if(m_complete_fired || !m_open || !m_ssl || !::SSL_is_init_finished(m_ssl))
        return;
    // Fail-closed mutual-auth gate: a verified peer cert is mandatory.
    X509 *peer    = ::SSL_get1_peer_certificate(m_ssl);
    const bool ok = peer && ::SSL_get_verify_result(m_ssl) == X509_V_OK;
    if(ok)
    {
        capture_peer_identity();
        // OpenSSL resets the MTU to the DTLS minimum during the handshake (a memory-BIO pair cannot discover the path
        // MTU), which would collapse DTLS_get_data_mtu to its ~219-byte floor.
        ::SSL_set_mtu(m_ssl, dtls_mtu());
        m_complete_fired = true;
        m_gate.mark_ready(); // the ready edge: send() now passes through
        if(peer)
            ::X509_free(peer);
        if(m_on_external_complete_cb)
            m_on_external_complete_cb();
        return;
    }
    if(peer)
        ::X509_free(peer);
    fail(io::io_error::connection_refused); // fail-closed: no completion
}

void dtls_channel::capture_peer_identity()
{
    m_node_id   = io::security::to_node_id(m_peer_facts);
    m_node_name = m_peer_facts.subject;
}

void dtls_channel::arm_retransmit()
{
    if(!m_ssl || !m_open)
        return;
    struct timeval tv{};
    if(::DTLSv1_get_timeout(m_ssl, &tv) != 1)
        return;
    const auto ms = std::chrono::milliseconds(static_cast<std::int64_t>(tv.tv_sec) * 1000 + static_cast<std::int64_t>(tv.tv_usec) / 1000);
    m_retransmit.cancel();
    m_retransmit.expires_after(ms);
    m_ops.begin();
    m_retransmit.async_wait(
            [this](std::error_code ec)
            {
                m_ops.end();
                if(ec || !m_open)
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
    if(m_open)
        shut_down();
}

void dtls_channel::fail(io::io_error e)
{
    if(!m_open)
        return;
    m_open = false;
    if(auto on_error = std::exchange(m_on_error_cb, nullptr))
        on_error(e);
    shut_down();
}

// Closing runs once and nothing begins after it. The built reassembler is held as one operation until its own waits
// settle, so on_closed is posted by whichever completion drains the count last.
void dtls_channel::shut_down()
{
    m_open                 = false;
    m_on_data_cb           = nullptr;
    m_on_error_cb          = nullptr;
    m_on_protocol_close_cb = nullptr;
    m_retransmit.cancel();
    plexus::asio::detail::udp_hold_until_settled(m_ops, m_reassembler);
    plexus::asio::detail::udp_cancel(m_reassembler);
    if(auto leave_demux = std::exchange(m_on_teardown_cb, nullptr))
        leave_demux();
    m_ops.close();
}

void dtls_channel::notify_closed()
{
    if(m_ops.settled())
        m_closed.notify([this](auto closure) { ::asio::post(m_io, std::move(closure)); });
}

void dtls_channel::post_on_data(std::span<const std::byte> frame)
{
    if(!m_open)
        return;
    m_ops.begin();
    auto owned = std::make_shared<std::vector<std::byte>>(frame.begin(), frame.end());
    ::asio::post(m_io,
                 [this, owned]
                 {
                     m_ops.end();
                     if(m_on_data_cb)
                         m_on_data_cb(std::span<const std::byte>{*owned});
                 });
}

void dtls_channel::feed_fragment(std::span<const std::byte> frame)
{
    ensure_reassembler();
    auto h = wire::decode_udp_fragment_header(frame);
    if(!h)
        return; // malformed sub-header: drop
    m_reassembler->feed(h->msg_id, h->frag_idx, h->frag_cnt, h->payload);
}

void dtls_channel::ensure_reassembler()
{
    if(m_reassembler)
        return;
    m_reassembler = std::make_unique<reassembler_type>(
            m_io, reassembler_type::config{.max_message_size = m_max_message_bytes, .total_memory_cap = m_reassembly_budget, .per_message_timeout = m_reassembly_timeout});
    m_reassembler->on_deliver([this](std::span<const std::byte> msg) { post_on_data(msg); });
}

io::endpoint dtls_channel::remote_endpoint() const
{
    return {"dtls", m_dest.address().to_string() + ":" + std::to_string(m_dest.port())};
}

}
