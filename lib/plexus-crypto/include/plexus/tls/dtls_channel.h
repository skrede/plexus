#ifndef HPP_GUARD_PLEXUS_TLS_DTLS_CHANNEL_H
#define HPP_GUARD_PLEXUS_TLS_DTLS_CHANNEL_H

#include "plexus/tls/tls_credential.h"
#include "plexus/tls/dtls_cookie.h"
#include "plexus/tls/detail/dtls_context.h"

#include "plexus/io/security/cert_facts.h"

#include "plexus/asio/udp_server.h"
#include "plexus/asio/asio_timer.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/mtu_budget.h"
#include "plexus/io/byte_channel.h"
#include "plexus/io/fragmentation.h"
#include "plexus/io/detail/handshake_gate.h"
#include "plexus/io/detail/reassembler.h"
#include "plexus/detail/compat.h"

#include "plexus/node_id.h"

#include "plexus/wire/udp_envelope.h"
#include "plexus/wire/stream_inbound.h"

#include <asio/io_context.hpp>
#include <asio/ip/udp.hpp>

#include <span>
#include <array>
#include <string>
#include <memory>
#include <vector>
#include <utility>
#include <cstddef>
#include <cstdint>

// Forward-declared OpenSSL handle: the header never sees a complete SSL — only
// dtls_channel.cpp includes <openssl/ssl.h>, so OpenSSL stays out of every
// consumer translation unit (the seam split tls_credential.h uses).
struct ssl_st;

namespace plexus::tls {

// byte_channel for the secure-best_effort DTLS datagram transport: a per-peer
// facade over the shared UDP socket (udp_channel's single-owner lifetime), driving
// an OpenSSL DTLS 1.2 state machine through a BIO pair (there is no asio DTLS stream
// wrapper, so the record pump lives in the .cpp with raw OpenSSL). Single-owner,
// bare `this`, posted on_data — NO shared_from_this, NO strand.
//
//   Inbound:  deliver_inbound(datagram) -> BIO_write(external) -> SSL_read drain;
//             each decrypted app frame posts on_data only after init-finished.
//   Outbound: SSL_write -> BIO_read(external) drain -> server.send_to(span, dest);
//             drain_outbound is called UNCONDITIONALLY after every SSL_write/SSL_read.
//   Complete: fires ONCE on SSL_is_init_finished AND verify_result==X509_V_OK AND a
//             non-null peer cert -> on_external_complete; else fail-closed.
//   Retransmit: DTLSv1_get_timeout arms one asio_timer; on fire
//             DTLSv1_handle_timeout + drain + re-arm; the dtor cancels it FIRST.
//
// Identity: after completion the peer SPKI digest is node_id and the cert
// subject is node_name. NO plexus wire frame crosses the datagram channel.
class dtls_channel
{
public:
    enum class role { client, server };

    using reassembler_type = io::detail::reassembler<::asio::io_context &, plexus::asio::asio_timer>;

    // The per-channel LOGICAL payload ceiling — the upper term of the
    // min(max_payload, DTLS_get_data_mtu) oversize cap. A caller MAY raise it above the
    // single-datagram budget so the fragment path carries large messages; the default is
    // the conservative single-Ethernet-datagram floor (parity with the pre-fragment cap).
    static constexpr std::size_t default_max_payload = io::mtu_budget{}.max_payload;

    // The per-channel DTLS RECORD MTU handed to SSL_set_mtu at BOTH set-points (the
    // construction edge AND the post-handshake completion re-assert). Unpinned from
    // the former fixed k_dtls_mtu to a configurable per-channel value; the default stays the
    // single-Ethernet-datagram floor so DTLS_get_data_mtu reports the real per-record fit
    // (the encrypted budget the fragmenter splits against). It is DECOUPLED from max_payload:
    // raising the logical ceiling (max_payload) lets a large message fragment, while each
    // fragment still rides one DTLS record bounded by this MTU.
    static constexpr std::size_t default_record_mtu = io::mtu_budget{}.max_payload;

    // Build over the shared udp_server: own NO socket, share the credential's
    // SSL_CTX (up_ref'd), publish the per-instance peer-addr + the transport's
    // cookie state into the SSL ex_data, and set accept/connect state per role. The
    // cookie_state is borrowed from the transport (one HMAC key per node); the
    // peer-addr block is this channel's own member.
    dtls_channel(::asio::io_context &io, plexus::asio::udp_server &server,
                 ::asio::ip::udp::endpoint dest, const tls_credential &cred,
                 io::security::cookie_secret &cookie_state, role r,
                 std::size_t max_payload = default_max_payload,
                 std::size_t record_mtu = default_record_mtu);

    dtls_channel(const dtls_channel &) = delete;
    dtls_channel &operator=(const dtls_channel &) = delete;
    dtls_channel(dtls_channel &&) = delete;
    dtls_channel &operator=(dtls_channel &&) = delete;

    // The dtor cancels the retransmit timer FIRST, then frees the external BIO
    // (SSL_free frees the internal BIO). No on_closed post (a this-capturing post
    // could outlive the channel — the owner quiesces the executor first).
    ~dtls_channel();

    // The seven byte_channel verbs.
    void send(std::span<const std::byte> bytes);
    void close();

    [[nodiscard]] io::endpoint remote_endpoint() const;

    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)> cb) { m_on_data = std::move(cb); }
    void on_closed(plexus::detail::move_only_function<void()> cb) { m_on_closed = std::move(cb); }
    void on_error(plexus::detail::move_only_function<void(io::io_error)> cb) { m_on_error = std::move(cb); }
    void on_protocol_close(plexus::detail::move_only_function<void(wire::close_cause)> cb) { m_on_protocol_close = std::move(cb); }

    // Fires ONCE on a verified mutual completion (the transport resolves the FSM
    // via handshake_fsm::on_external_complete from this edge). Set before driving.
    void on_external_complete(plexus::detail::move_only_function<void()> cb) { m_on_external_complete = std::move(cb); }

    // Kick the handshake: synthesize the ClientHello (client) / arm accept (server),
    // drain the BIO to the wire, and arm the retransmit timer. The transport calls
    // this after wiring the callbacks (server: then feeds the triggering datagram).
    void start_handshake();

    // Called BY the transport demux on each datagram for this peer (NOT a self-run
    // recv loop — the channel owns no socket): feed the external BIO + drive SSL_read.
    void deliver_inbound(std::span<const std::byte> datagram);

    [[nodiscard]] const ::asio::ip::udp::endpoint &dest() const noexcept { return m_dest; }
    [[nodiscard]] bool is_open() const noexcept { return m_open; }
    [[nodiscard]] bool complete() const noexcept { return m_complete_fired; }

    // The cert-derived peer identity, valid only after on_external_complete fired.
    [[nodiscard]] const node_id &peer_node_id() const noexcept { return m_node_id; }
    [[nodiscard]] const std::string &peer_node_name() const noexcept { return m_node_name; }

private:
    // The configurable SSL_set_mtu record budget (both set-points consult this so unpinning
    // honors the ctor value at construction AND at the post-handshake completion re-assert).
    [[nodiscard]] long dtls_mtu() const noexcept { return static_cast<long>(m_record_mtu); }

    void secure_send(std::span<const std::byte> bytes);
    // The post-handshake encrypted per-record budget DTLS_get_data_mtu reports (the real
    // per-record fit), capped by the configured logical ceiling. The fragmenter splits
    // against THIS, not the configured ceiling (so each fragment rides one DTLS record).
    [[nodiscard]] std::size_t record_budget() const noexcept;
    // Split an oversize-but-fragmentable frame across numbered records: each fragment is one
    // FRAGMENTED-bit envelope (the 9-byte sub-header + slice), submitted through the gate to
    // secure_send. A frame beyond the bounded max-message size is rejected, not fragmented.
    void send_large(std::span<const std::byte> bytes);
    // A decrypted inbound record carrying the FRAGMENTED envelope: decode its sub-header
    // (fail-closed) and feed the bounded reassembler; a completed message posts on_data.
    void feed_fragment(std::span<const std::byte> frame);
    void ensure_reassembler();
    void drain_outbound();
    void drain_inbound();
    void try_complete();
    void arm_retransmit();
    void on_retransmit();
    void fail(io::io_error e);
    void post_on_data(std::span<const std::byte> frame);
    void publish_cookie_ex_data();
    void capture_peer_identity();

    ::asio::io_context &m_io;
    plexus::asio::udp_server &m_server;
    ::asio::ip::udp::endpoint m_dest;
    io::security::cookie_secret &m_cookie_state;
    role m_role;
    std::size_t m_max_payload;
    std::size_t m_record_mtu;

    detail::shared_ssl_ctx m_ssl_ctx;            // up_ref'd shared SSL_CTX
    ssl_st *m_ssl{nullptr};
    void *m_external_bio{nullptr};               // BIO* (opaque in the header)
    plexus::asio::asio_timer m_retransmit;

    // The open-before-data gate, composed in DROP-PRESERVING / ready-edge mode: a
    // pre-ready send is DROPPED (send() never submits before the ready edge), so the
    // gate's buffer always stays empty here — it is NOT the enqueue-then-drain variant.
    // try_complete() flips the ready edge via mark_ready; after that send() forwards
    // straight through the gate to the post-ready SSL_write -> drain_outbound egress
    // (the drain installed at construction).
    io::detail::handshake_gate m_gate;

    std::vector<unsigned char> m_peer_addr_block; // [len][addr bytes] for the cookie cb
    std::array<unsigned char, 2048> m_drain_buf{};

    // The fragment/reassemble blocks, composed as owned members the channel drives directly
    // (the assembly pattern): the splitter encodes each fragment into m_frag_scratch (reused,
    // no per-send alloc); the bounded reassembler (built lazily on the first inbound fragment)
    // sits on the decrypted drain_inbound path. Its per-message timeout timer is cancelled in
    // the dtor FIRST (before m_gate.reset() / the OpenSSL teardown) so a timer firing after
    // teardown is a guarded no-op (single-owner, no use-after-free, no shared_from_this).
    std::vector<std::byte> m_frag_scratch;        // reused fragment-encode buffer
    std::uint16_t m_out_msg_id{0};                // per-message fragment grouping id (sender)
    std::unique_ptr<reassembler_type> m_reassembler;

    // The verify-time depth-0 leaf facts the verify callback stashes here through the
    // per-instance SSL ex_data slot (the ONE cert extraction per handshake). The
    // completion edge derives node_id (first 16 bytes of spki_sha256) + node_name
    // (subject) from this value — no second SPKI digest at the capture site.
    io::security::cert_facts m_peer_facts;
    node_id m_node_id{};
    std::string m_node_name;

    plexus::detail::move_only_function<void(std::span<const std::byte>)> m_on_data;
    plexus::detail::move_only_function<void()> m_on_closed;
    plexus::detail::move_only_function<void(io::io_error)> m_on_error;
    plexus::detail::move_only_function<void(wire::close_cause)> m_on_protocol_close;
    plexus::detail::move_only_function<void()> m_on_external_complete;

    bool m_open{true};
    bool m_complete_fired{false};
};

}

static_assert(plexus::io::byte_channel<plexus::tls::dtls_channel>,
    "dtls_channel must satisfy byte_channel — check the send/close/on_* surface");

#endif
