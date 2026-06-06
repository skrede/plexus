#ifndef HPP_GUARD_PLEXUS_ASIO_UDP_CHANNEL_H
#define HPP_GUARD_PLEXUS_ASIO_UDP_CHANNEL_H

#include "plexus/asio/udp_server.h"

#include "plexus/wire/udp_envelope.h"
#include "plexus/wire/udp_dedup_window.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/byte_channel.h"
#include "plexus/detail/compat.h"

#include <asio/post.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/udp.hpp>

#include <span>
#include <string>
#include <vector>
#include <memory>
#include <utility>
#include <cstddef>
#include <cstdint>

namespace plexus::asio {

// The connectionless UDP byte_channel: plexus's first NON-STREAM channel. Unlike
// every stream channel (one kernel socket per connection) a udp_channel owns NO
// socket — it is a per-peer facade over the ONE router-owned udp_server, storing the
// destination endpoint plus the per-peer envelope/dedup state. send() wraps the
// frame in a udp_envelope (seq++, kind=best_effort) and calls server.send_to(dest);
// inbound is PUSHED in by the transport demux (deliver_inbound), not pulled by a
// self-run recv loop. The seven byte_channel verbs hold without reshaping the
// concept (the static_assert at file bottom is the load-bearing D2 proof).
//
// DIVERGENCE from the stream channels: no stream_inbound / frame_reassembler /
// slowloris timer (a datagram is a complete message — nothing to reassemble, no
// partial-frame stall to detect) and no write-queue drain (a datagram send is
// one-shot). on_protocol_close is STORED and NEVER fired: the byte_channel concept
// licenses this for a non-stream channel ("no partial frame is expressible without a
// byte stream", byte_channel.h:43-46) — a malformed datagram is simply dropped.
//
// Oversize (D-03): a frame whose enveloped size exceeds max_payload is REJECTED at
// publish through on_error(message_too_large), never silently dropped — the channel
// stays open and the publisher learns the message will not send.
//
// The reliable_arq kind is a recv-side hook only this plan (the data ARQ is a later
// block); a kind=1 datagram is handed to m_on_reliable, which the transport leaves
// unset here so such datagrams are dropped until the ARQ engine is wired.
class udp_channel
{
public:
    static constexpr std::size_t default_max_payload = 1400;

    udp_channel(::asio::io_context &io, udp_server &server, ::asio::ip::udp::endpoint dest,
                std::size_t max_payload = default_max_payload)
        : m_io(io)
        , m_server(server)
        , m_dest(std::move(dest))
        , m_max_payload(max_payload)
    {
    }

    udp_channel(const udp_channel &) = delete;
    udp_channel &operator=(const udp_channel &) = delete;
    udp_channel(udp_channel &&) = delete;
    udp_channel &operator=(udp_channel &&) = delete;

    // The dtor tears the channel down but never posts on_closed (a this-capturing
    // post could outlive the channel). close() posts on_closed.
    ~udp_channel() { m_open = false; }

    void send(std::span<const std::byte> frame)
    {
        if(!m_open)
            return;
        if(frame.size() + wire::udp_envelope_overhead > m_max_payload)
            return reject_oversize();
        wire::wrap_udp_into(m_send_scratch, wire::udp_envelope_kind::best_effort, m_out_seq++, frame);
        m_server.send_to(m_send_scratch, m_dest);
    }

    void close()
    {
        if(!m_open)
            return;
        m_open = false;
        ::asio::post(m_io, [this] { if(m_on_closed) m_on_closed(); });   // posted, never synchronous
    }

    [[nodiscard]] io::endpoint remote_endpoint() const
    {
        return {"udp", m_dest.address().to_string() + ":" + std::to_string(m_dest.port())};
    }

    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)> cb) { m_on_data = std::move(cb); }
    void on_closed(plexus::detail::move_only_function<void()> cb) { m_on_closed = std::move(cb); }
    void on_error(plexus::detail::move_only_function<void(io::io_error)> cb) { m_on_error = std::move(cb); }
    void on_protocol_close(plexus::detail::move_only_function<void(wire::close_cause)> cb) { m_on_protocol_close = std::move(cb); }

    // The reliable-ARQ recv hook (kind=1). Unset this plan — the data ARQ is a later
    // block; until then a reliable datagram is dropped. The seam keeps ONE inbound
    // demux path for both kinds (the kind discriminator is also the DTLS-bypass seam).
    void on_reliable_segment(plexus::detail::move_only_function<void(std::uint16_t, std::span<const std::byte>)> cb)
    {
        m_on_reliable = std::move(cb);
    }

    // Called BY the transport demux on each datagram for this peer — NOT a self-run
    // recv loop (the channel owns no socket). Strip the envelope, dedup best_effort,
    // post the inner frame. A malformed datagram is dropped (no on_protocol_close).
    void deliver_inbound(std::span<const std::byte> datagram)
    {
        auto dec = wire::unwrap_udp(datagram);
        if(!dec)
            return;                                  // malformed: drop, never on_protocol_close
        if(dec->kind == wire::udp_envelope_kind::best_effort)
        {
            if(m_dedup.admit(dec->seq) != wire::udp_dedup_window::outcome::fresh)
                return;                              // duplicate / too_old: drop
            post_on_data(dec->frame);
        }
        else if(m_on_reliable)
        {
            m_on_reliable(dec->seq, dec->frame);     // reliable ARQ engine (later block)
        }
    }

    [[nodiscard]] const ::asio::ip::udp::endpoint &dest() const noexcept { return m_dest; }
    [[nodiscard]] bool is_open() const noexcept { return m_open; }

private:
    void reject_oversize()
    {
        if(m_on_error)
            m_on_error(io::io_error::message_too_large);
    }

    // on_data is ALWAYS posted (the byte_channel contract). The owning vector keeps
    // the bytes alive across the post (the demux's recv buffer is reused immediately).
    void post_on_data(std::span<const std::byte> frame)
    {
        auto owned = std::make_shared<std::vector<std::byte>>(frame.begin(), frame.end());
        ::asio::post(m_io, [this, owned]
        {
            if(m_on_data)
                m_on_data(std::span<const std::byte>{*owned});
        });
    }

    ::asio::io_context &m_io;
    udp_server &m_server;
    ::asio::ip::udp::endpoint m_dest;
    std::size_t m_max_payload;
    std::uint16_t m_out_seq{0};
    wire::udp_dedup_window m_dedup;
    std::vector<std::byte> m_send_scratch;
    plexus::detail::move_only_function<void(std::span<const std::byte>)> m_on_data;
    plexus::detail::move_only_function<void()> m_on_closed;
    plexus::detail::move_only_function<void(io::io_error)> m_on_error;
    plexus::detail::move_only_function<void(wire::close_cause)> m_on_protocol_close;
    plexus::detail::move_only_function<void(std::uint16_t, std::span<const std::byte>)> m_on_reliable;
    bool m_open{true};
};

}

static_assert(plexus::io::byte_channel<plexus::asio::udp_channel>,
    "udp_channel must satisfy byte_channel WITHOUT reshaping the concept — the NON-stream D2 proof");

#endif
