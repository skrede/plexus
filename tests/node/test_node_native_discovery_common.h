// The two-node native-discovery oracle: two real basic_node instances, each with a
// multicast_discovery over a datagram_socket, discovering each other with no static table and no
// mdnspp. The live path rides the real udp_multicast_socket; a paired wiring fake delivers the
// same node->discovery->note_from_card path deterministically without real multicast.
#pragma once

#include "plexus/native/multicast_discovery.h"

#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_transport.h"
#include "plexus/asio/udp_multicast_socket.h"

#include "plexus/node.h"
#include "plexus/node_options.h"

#include "plexus/stream/datagram_socket.h"
#include "plexus/detail/compat.h"

#include <asio/post.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/udp.hpp>
#include <asio/ip/address_v4.hpp>

#include <span>
#include <string>
#include <vector>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>
#include <functional>
#include <system_error>

namespace node_native_discovery_fixture {

namespace pasio = plexus::asio;

using asio_node = plexus::basic_node<pasio::asio_policy, pasio::asio_transport>;

inline plexus::node_options make_opts()
{
    plexus::node_options opts;
    opts.redial_seed = 0xC0FFEEu;
    return opts;
}

template<typename Pred>
inline void pump_until(::asio::io_context &io, Pred pred)
{
    const auto bound = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while(!pred() && std::chrono::steady_clock::now() < bound)
    {
        io.poll();
        if(io.stopped())
            io.restart();
    }
}

// A wiring datagram_socket: send_multicast posts each payload onto the io_context as an inbound
// delivery to its paired peer's on_datagram, with a synthesized loopback source endpoint. It
// satisfies stream::datagram_socket and uses the asio udp endpoint, so the discovery's source-IP
// path (from.address().to_string()) runs exactly as on the live socket — deterministically, with
// no real multicast.
class wiring_datagram_socket
{
public:
    using endpoint_type = ::asio::ip::udp::endpoint;

    explicit wiring_datagram_socket(::asio::io_context &io, std::string source_host)
            : m_io(io)
            , m_source(std::move(source_host))
    {
    }

    void pair_with(wiring_datagram_socket &peer)
    {
        m_peer = peer;
    }

    std::error_code bind(const endpoint_type &)
    {
        return {};
    }

    void send_multicast(std::span<const std::byte> bytes)
    {
        if(!m_peer)
            return;
        wiring_datagram_socket &peer = m_peer->get();
        std::vector<std::byte> owned(bytes.begin(), bytes.end());
        const std::string from = m_source;
        ::asio::post(m_io, [&peer, from, owned = std::move(owned)]() { peer.deliver(from, owned); });
    }

    void on_datagram(plexus::detail::move_only_function<void(const endpoint_type &, std::span<const std::byte>)> cb)
    {
        m_on_datagram = std::move(cb);
    }

    void close()
    {
    }

    void deliver(const std::string &from_host, std::span<const std::byte> bytes)
    {
        if(m_on_datagram)
            m_on_datagram(endpoint_type{::asio::ip::make_address_v4(from_host), 7447}, bytes);
    }

private:
    ::asio::io_context &m_io;
    std::string m_source;
    std::optional<std::reference_wrapper<wiring_datagram_socket>> m_peer;
    plexus::detail::move_only_function<void(const endpoint_type &, std::span<const std::byte>)> m_on_datagram;
};

static_assert(plexus::stream::datagram_socket<wiring_datagram_socket>);

} // namespace node_native_discovery_fixture
