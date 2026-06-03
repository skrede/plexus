#ifndef HPP_GUARD_PLEXUS_ASIO_ASIO_CHANNEL_H
#define HPP_GUARD_PLEXUS_ASIO_ASIO_CHANNEL_H

#include "plexus/asio/detail/asio_error_map.h"

#include "plexus/wire/frame_codec.h"
#include "plexus/wire/frame_reassembler.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/detail/compat.h"

#include <asio/post.hpp>
#include <asio/write.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/io_context.hpp>

#include <deque>
#include <span>
#include <array>
#include <memory>
#include <vector>
#include <string>
#include <utility>
#include <cstddef>
#include <system_error>

namespace plexus::asio {

// TCP byte_channel over ::asio::ip::tcp::socket. Inbound bytes feed a member
// wire::frame_reassembler; each complete frame's payload is POSTED to on_data
// (never delivered inline from the read handler, per the byte_channel contract).
// send() async-writes through an unbounded write queue (sufficient for the slice
// per RESEARCH §1.2; QoS/backpressure are out of scope). The channel is
// caller-owned and runs its read loop with `this` captured, mirroring the inproc
// sibling — the owner must outlive any pending async op. close() cancels the
// socket and posts on_closed; native errors map to io_error and post to on_error.
class asio_channel
{
public:
    explicit asio_channel(::asio::io_context &io)
        : m_io(io)
        , m_socket(io)
    {
    }

    asio_channel(::asio::io_context &io, std::error_code &)
        : asio_channel(io)
    {
    }

    // Accept-mode: adopt an already-connected socket and start reading. The
    // listener mints this once the connection is accepted.
    asio_channel(::asio::io_context &io, ::asio::ip::tcp::socket connected)
        : m_io(io)
        , m_socket(std::move(connected))
    {
        m_open = true;
        do_read();
    }

    // The dtor only tears the socket down — it never posts on_closed, since a
    // this-capturing post could outlive the channel. A user-initiated close()
    // posts on_closed; teardown via destruction is silent.
    ~asio_channel() { shutdown_socket(); }

    asio_channel(const asio_channel &) = delete;
    asio_channel &operator=(const asio_channel &) = delete;
    asio_channel(asio_channel &&) = delete;
    asio_channel &operator=(asio_channel &&) = delete;

    void send(std::span<const std::byte> data)
    {
        if(!m_open)
            return;
        m_write_queue.emplace_back(data.begin(), data.end());
        if(!m_writing)
            do_write();
    }

    void close()
    {
        if(!m_socket.is_open())
            return;
        shutdown_socket();
        // Posted, never synchronous, per the byte_channel contract. The owner
        // must outlive the post (the same lifetime discipline as the read loop).
        ::asio::post(m_io, [this] { if(m_on_closed) m_on_closed(); });
    }

    [[nodiscard]] io::endpoint remote_endpoint() const
    {
        std::error_code ec;
        auto ep = m_socket.remote_endpoint(ec);
        if(ec)
            return {"tcp", ""};
        return {"tcp", ep.address().to_string() + ":" + std::to_string(ep.port())};
    }

    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)> cb) { m_on_data = std::move(cb); }
    void on_closed(plexus::detail::move_only_function<void()> cb) { m_on_closed = std::move(cb); }
    void on_error(plexus::detail::move_only_function<void(io::io_error)> cb) { m_on_error = std::move(cb); }

    [[nodiscard]] ::asio::ip::tcp::socket &socket() noexcept { return m_socket; }
    void start_read() { m_open = true; do_read(); }

private:
    void shutdown_socket()
    {
        if(!m_socket.is_open())
            return;
        std::error_code ec;
        (void)m_socket.shutdown(::asio::ip::tcp::socket::shutdown_both, ec);
        (void)m_socket.close(ec);
        m_open = false;
    }

    void do_read()
    {
        m_socket.async_read_some(::asio::buffer(m_read_buf),
            [this](std::error_code ec, std::size_t n)
            {
                if(ec)
                    return fail(ec);
                feed_inbound(std::span<const std::byte>{m_read_buf.data(), n});
                do_read();
            });
    }

    void feed_inbound(std::span<const std::byte> bytes)
    {
        auto result = m_reassembler.feed(bytes);
        for(auto &frame : result.frames)
            post_frame(frame);
    }

    // Re-frame the reassembled frame back into a COMPLETE header-on frame and post
    // THAT, so on_data delivers a full frame (header + payload) — identical in
    // shape to what the inproc channel delivers (send() bytes verbatim) — and the
    // downstream frame_router sees the frame_header.type byte. The reassembler
    // split the header off; this stitches it back on. The re-frame runs into a
    // reused member scratch (encode_frame_into reuses capacity, no per-frame
    // alloc on the steady path); the owning vector keeps the bytes alive across
    // the post, the same lifetime discipline as before.
    void post_frame(const wire::complete_frame &frame)
    {
        wire::encode_frame_into(m_frame_scratch, frame.header, frame.payload);
        auto owned = std::make_shared<std::vector<std::byte>>(m_frame_scratch);
        ::asio::post(m_io, [this, owned]
        {
            if(m_on_data)
                m_on_data(std::span<const std::byte>{*owned});
        });
    }

    void do_write()
    {
        if(m_write_queue.empty())
        {
            m_writing = false;
            return;
        }
        m_writing = true;
        ::asio::async_write(m_socket, ::asio::buffer(m_write_queue.front()),
            [this](std::error_code ec, std::size_t)
            {
                m_write_queue.pop_front();
                if(ec)
                    return fail(ec);
                do_write();
            });
    }

    void fail(const std::error_code &ec)
    {
        if(ec == ::asio::error::operation_aborted || !m_open)
            return;
        m_open = false;
        m_writing = false;
        auto mapped = detail::map_error(ec);
        if(m_on_error)
            m_on_error(mapped);
        if(m_on_closed)
            m_on_closed();
    }

    ::asio::io_context &m_io;
    ::asio::ip::tcp::socket m_socket;
    wire::frame_reassembler m_reassembler;
    std::vector<std::byte> m_frame_scratch;
    std::array<std::byte, 4096> m_read_buf{};
    std::deque<std::vector<std::byte>> m_write_queue;
    plexus::detail::move_only_function<void(std::span<const std::byte>)> m_on_data;
    plexus::detail::move_only_function<void()> m_on_closed;
    plexus::detail::move_only_function<void(io::io_error)> m_on_error;
    bool m_open{false};
    bool m_writing{false};
};

}

#endif
