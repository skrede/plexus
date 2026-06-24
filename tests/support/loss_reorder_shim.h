#ifndef HPP_GUARD_PLEXUS_TESTS_SUPPORT_LOSS_REORDER_SHIM_H
#define HPP_GUARD_PLEXUS_TESTS_SUPPORT_LOSS_REORDER_SHIM_H

#include "support/loss_reorder_scheduler.h"

#include <asio/buffer.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/udp.hpp>

#include <array>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>

namespace plexus::testing {

// A live UDP relay applying the deterministic loss_reorder_scheduler at the wire boundary between
// a client and a server socket: each client->server datagram is forwarded, dropped, or held and
// re-emitted out of order; replies are piped back untouched (the loss model is one-directional on
// the client->server leg). The schedule is byte-identical across runs (the empirical-
// reproducibility property the fragment sweep and lossy-link bench cell depend on; no std::random).
class loss_reorder_relay
{
public:
    loss_reorder_relay(::asio::io_context &io, std::uint16_t server_port, loss_reorder_config cfg)
            : m_io(io)
            , m_front(io, ::asio::ip::udp::endpoint(::asio::ip::udp::v4(), 0))
            , m_back(io, ::asio::ip::udp::endpoint(::asio::ip::udp::v4(), 0))
            , m_server_ep(::asio::ip::make_address("127.0.0.1"), server_port)
            , m_sched(cfg)
    {
        recv_front();
        recv_back();
    }

    std::uint16_t port() const
    {
        return m_front.local_endpoint().port();
    }
    std::size_t dropped() const noexcept
    {
        return m_sched.dropped();
    }
    std::size_t forwarded() const noexcept
    {
        return m_forwarded;
    }

private:
    void emit_to_server(std::vector<std::byte> dg)
    {
        ++m_forwarded;
        m_back.send_to(::asio::buffer(dg.data(), dg.size()), m_server_ep);
    }

    void recv_front()
    {
        m_front.async_receive_from(::asio::buffer(m_front_buf), m_from,
                                   [this](std::error_code ec, std::size_t n)
                                   {
                                       if(ec)
                                           return;
                                       m_client_ep = m_from;
                                       for(auto &dg : m_sched.drive(std::span<const std::byte>{m_front_buf.data(), n}))
                                           emit_to_server(std::move(dg));
                                       recv_front();
                                   });
    }

    void recv_back()
    {
        m_back.async_receive_from(::asio::buffer(m_back_buf), m_from,
                                  [this](std::error_code ec, std::size_t n)
                                  {
                                      if(ec)
                                          return;
                                      if(m_client_ep.port() != 0)
                                          m_front.send_to(::asio::buffer(m_back_buf.data(), n), m_client_ep);
                                      recv_back();
                                  });
    }

    ::asio::io_context &m_io;
    ::asio::ip::udp::socket m_front;
    ::asio::ip::udp::socket m_back;
    ::asio::ip::udp::endpoint m_server_ep;
    ::asio::ip::udp::endpoint m_client_ep;
    ::asio::ip::udp::endpoint m_from;
    std::array<std::byte, 65536> m_front_buf{};
    std::array<std::byte, 65536> m_back_buf{};
    loss_reorder_scheduler m_sched;
    std::size_t m_forwarded = 0;
};

}

#endif
