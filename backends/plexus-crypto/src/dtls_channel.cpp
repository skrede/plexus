#include "plexus/tls/dtls_channel.h"

#include "plexus/tls/detail/dtls_identity.h"

#include <openssl/bio.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>

#include <chrono>
#include <cstring>
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
{
    // Hold the largest single DTLS record either direction in ONE buffer: the configured
    // record budget plus headroom for the 13-byte record header and worst-case cipher
    // expansion (IV/tag/block padding). Sizing from the knob (not a 2048 constant) keeps
    // SSL_read from discarding and BIO_read from splitting a record once record_mtu rises.
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
    m_open = false;
    m_retransmit.cancel(); // cancel the timer FIRST (no use-after-free on teardown)
    if(m_reassembler)
        m_reassembler->cancel(); // cancel the reassembly timeout(s) before teardown
    if(m_on_teardown_cb)
        m_on_teardown_cb(); // erase the transport demux ref BEFORE this object dies
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
    // Publish the facts-stash destination so the verify callback writes the ONE
    // verify-time leaf extraction back into this channel's m_peer_facts (the same
    // per-instance ex_data stitch, never thread_local).
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
    // DROP-PRESERVING gate: a send before the ready edge is DROPPED, never buffered
    // (the gate is composed in pass-through mode — m_gate.submit is never called before
    // mark_ready, so the buffer always stays empty). After the ready edge the gate
    // forwards straight through to secure_send (its installed drain).
    if(!m_open || !m_ssl || !m_gate.is_ready())
        return;
    // Every post-ready app send rides the UDP envelope so the receiver has an UNAMBIGUOUS
    // whole-vs-fragment discriminator (the FRAGMENTED bit) — an opaque user frame's first
    // byte cannot alias the discriminator, the failure a bare in-band marker would have. A
    // frame whose enveloped size fits ONE encrypted record rides one record; a larger frame
    // is SPLIT across numbered records (the per-record budget bounds each); only a frame
    // beyond the bounded max-message size is rejected.
    if(bytes.size() + wire::udp_envelope_overhead > record_budget())
        return send_large(bytes);
    wire::wrap_udp_into(m_frag_scratch, wire::udp_envelope_kind::best_effort, 0, bytes);
    m_gate.submit(std::span<const std::byte>{m_frag_scratch}); // ready -> pass straight to secure_send
}

std::size_t dtls_channel::record_budget() const noexcept
{
    // the encrypted per-record budget OpenSSL reports post-handshake (the real fit in
    // one DTLS record), capped by the configured logical ceiling — min(max_payload,
    // DTLS_get_data_mtu). This is the oversize-reject term AND the fragmenter's split budget.
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
        // Each fragment is one FRAGMENTED-bit envelope (kind is nominal — DTLS owns its own
        // anti-replay, so the seq field is unused) submitted through the ready gate; the per-
        // record budget bounds the wrapped fragment to one DTLS record (drop-whole on loss).
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
        // Send DIRECTLY from the owned drain buffer (sized at construction from the record
        // budget, no per-datagram scratch reallocation on the steady-state hot path):
        // udp_server's outbound queue owns each in-flight datagram's bytes across its
        // async_send_to, so the buffer is free to be overwritten by the next BIO_read on return.
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
                m_open = false;
                m_retransmit.cancel();
                // Post off the current stack, mirroring close(): a consumer that
                // destroys the channel in its on_closed handler (the natural reaction
                // to a peer close) would otherwise free `this` mid-drain_inbound,
                // then the loop / try_complete() touches freed members (a UAF). The
                // synchronous m_on_error_cb in fail() is only safe because the transport's
                // error handlers defer destruction; the close path has no such deferral.
                ::asio::post(m_io,
                             [this]
                             {
                                 if(m_on_closed_cb)
                                     m_on_closed_cb();
                             });
                break;
            }
            drain_outbound(); // flush any fatal alert to the peer first
            fail(io::io_error::broken_pipe);
            break;
        }
        const std::span<const std::byte> plaintext{reinterpret_cast<const std::byte *>(m_drain_buf.data()), static_cast<std::size_t>(n)};
        // Strip the envelope every send wraps: the FRAGMENTED bit is the unambiguous
        // discriminator. A fragment record feeds the reassembler (a completed message posts
        // on_data); a whole record posts its inner frame directly. A malformed record (under
        // the envelope overhead) is dropped — never indexed past the span (fail-closed).
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
    if(m_complete_fired || !m_ssl || !::SSL_is_init_finished(m_ssl))
        return;
    // Fail-closed mutual-auth gate: a verified peer cert is mandatory.
    X509 *peer    = ::SSL_get1_peer_certificate(m_ssl);
    const bool ok = peer && ::SSL_get_verify_result(m_ssl) == X509_V_OK;
    if(ok)
    {
        capture_peer_identity(); // identity from the stashed verify-time facts
        // OpenSSL resets the SSL MTU to the conservative DTLS minimum during the
        // handshake (a memory-BIO pair cannot do path-MTU discovery), so re-assert the
        // configured record budget now that the cipher is negotiated — DTLS_get_data_mtu
        // (the send() oversize-reject term) otherwise collapses to the ~219-byte floor
        // instead of the intended ~1400 budget.
        ::SSL_set_mtu(m_ssl, dtls_mtu()); // completion edge: re-assert the configured budget
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
    // The peer identity comes from the ONE verify-time cert_facts extraction the
    // verify callback stashed into m_peer_facts — no second SPKI digest, no X509
    // re-parse at the completion edge. node_id is the first 16 bytes of the full
    // SPKI digest; node_name is the cert subject.
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
    m_retransmit.async_wait(
            [this](std::error_code ec)
            {
                if(ec) // operation_aborted on teardown self-guards
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
    ::asio::post(m_io,
                 [this]
                 {
                     if(m_on_closed_cb)
                         m_on_closed_cb();
                 }); // posted, never synchronous
}

void dtls_channel::fail(io::io_error e)
{
    if(!m_open)
        return;
    m_open = false;
    m_retransmit.cancel();
    if(m_on_error_cb)
        m_on_error_cb(e);
}

void dtls_channel::post_on_data(std::span<const std::byte> frame)
{
    auto owned = std::make_shared<std::vector<std::byte>>(frame.begin(), frame.end());
    ::asio::post(m_io,
                 [this, owned]
                 {
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
    // Sans-IO block: an assembled message POSTS on_data (the channel owns the post). The
    // reassembler's per-message timeout reclaims a stalled best-effort partial (DTLS app
    // records are unreliable, like UDP best_effort — a lost fragment drops the whole message).
    // The per-message ceiling, the aggregate budget, and the reclaim window are the threaded
    // node-options knobs, so a large message clears the receive bounds it must reach.
    m_reassembler = std::make_unique<reassembler_type>(
            m_io, reassembler_type::config{.max_message_size = m_max_message_bytes, .total_memory_cap = m_reassembly_budget, .per_message_timeout = m_reassembly_timeout});
    m_reassembler->on_deliver([this](std::span<const std::byte> msg) { post_on_data(msg); });
}

io::endpoint dtls_channel::remote_endpoint() const
{
    return {"dtls", m_dest.address().to_string() + ":" + std::to_string(m_dest.port())};
}

}
