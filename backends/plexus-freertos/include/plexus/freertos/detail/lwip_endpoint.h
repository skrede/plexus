#ifndef HPP_GUARD_PLEXUS_FREERTOS_DETAIL_LWIP_ENDPOINT_H
#define HPP_GUARD_PLEXUS_FREERTOS_DETAIL_LWIP_ENDPOINT_H

#include <string>
#include <utility>
#include <cstdint>

namespace plexus::freertos::detail {

// The datagram leaf's endpoint_type: the discovery template default-constructs it, sets the bound
// port through port(), and on inbound reads the source host via address().to_string(). The shape
// mirrors the asio sibling's ::asio::ip::udp::endpoint surface (port + address.to_string) but holds
// only a bare dotted-quad host string, so it is pure C++ and reaches no lwIP header — it builds on
// the host, which is what lets the seam TU prove the contract without a board.
class lwip_address
{
public:
    explicit lwip_address(std::string host)
            : m_host(std::move(host))
    {
    }

    std::string to_string() const
    {
        return m_host;
    }

private:
    std::string m_host;
};

class lwip_endpoint
{
public:
    lwip_endpoint()
            : m_host()
            , m_port(0)
    {
    }

    explicit lwip_endpoint(std::string host, std::uint16_t port = 0)
            : m_host(std::move(host))
            , m_port(port)
    {
    }

    lwip_address address() const
    {
        return lwip_address{m_host};
    }

    void port(std::uint16_t p)
    {
        m_port = p;
    }

    std::uint16_t port() const
    {
        return m_port;
    }

private:
    std::string   m_host;
    std::uint16_t m_port;
};

}

#endif
