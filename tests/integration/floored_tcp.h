#ifndef HPP_GUARD_PLEXUS_TESTS_INTEGRATION_FLOORED_TCP_H
#define HPP_GUARD_PLEXUS_TESTS_INTEGRATION_FLOORED_TCP_H

#include <asio/io_context.hpp>
#include <asio/socket_base.hpp>

#include <asio/ip/tcp.hpp>

#include <cstdint>

namespace floored_tcp {

inline constexpr std::int32_t k_receive_floor = 1024;

// Winsock completes any send it can copy while the socket is under its SO_SNDBUF quota, however large (Microsoft
// KB 214397), so only a zero quota keeps one large write pending; BSD stacks reject a zero SO_SNDBUF.
#if defined(_WIN32)
inline constexpr std::int32_t k_send_floor = 0;
#else
inline constexpr std::int32_t k_send_floor = 1024;
#endif

// The receive window is sized at the handshake (tcp(7)), so the floor goes on the listener and its accepts inherit it.
inline ::asio::ip::tcp::acceptor loopback_acceptor(::asio::io_context &io)
{
    ::asio::ip::tcp::acceptor acc{io};
    acc.open(::asio::ip::tcp::v4());
    acc.set_option(::asio::socket_base::receive_buffer_size{k_receive_floor});
    acc.bind(::asio::ip::tcp::endpoint{::asio::ip::make_address("127.0.0.1"), 0});
    acc.listen();
    return acc;
}

inline void connect(::asio::ip::tcp::socket &client, const ::asio::ip::tcp::endpoint &to)
{
    client.open(to.protocol());
    client.set_option(::asio::socket_base::send_buffer_size{k_send_floor});
    client.connect(to);
}

}

#endif
