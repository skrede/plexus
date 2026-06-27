// A deterministic fake datagram_socket + host policy for the multicast_discovery unit cases:
// the fake captures every send_multicast payload and lets a test inject an inbound (from, bytes)
// into the stored on_datagram handler, the unit injection point with no real socket or loop.
#pragma once

#include "plexus/native/multicast_discovery.h"

#include "plexus/stream/datagram_socket.h"
#include "plexus/detail/compat.h"

#include <span>
#include <string>
#include <vector>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <system_error>

namespace multicast_discovery_fixture {

struct fake_address
{
    std::string host;

    std::string to_string() const
    {
        return host;
    }
};

struct fake_endpoint
{
    fake_address addr;
    std::uint16_t portno = 0;

    fake_address address() const
    {
        return addr;
    }

    void port(std::uint16_t p)
    {
        portno = p;
    }
};

class fake_datagram_socket
{
public:
    using endpoint_type = fake_endpoint;

    std::error_code bind(const endpoint_type &)
    {
        return {};
    }

    void send_multicast(std::span<const std::byte> bytes)
    {
        sent.emplace_back(bytes.begin(), bytes.end());
    }

    void on_datagram(plexus::detail::move_only_function<void(const endpoint_type &, std::span<const std::byte>)> cb)
    {
        m_on_datagram = std::move(cb);
    }

    void close()
    {
        closed = true;
    }

    void inject(const std::string &from_host, std::span<const std::byte> bytes)
    {
        if(m_on_datagram)
            m_on_datagram(fake_endpoint{fake_address{from_host}, 7447}, bytes);
    }

    std::vector<std::vector<std::byte>> sent;
    bool closed = false;

private:
    plexus::detail::move_only_function<void(const endpoint_type &, std::span<const std::byte>)> m_on_datagram;
};

// A no-op timer: advertise arms it, but the unit cases assert the IMMEDIATE emit, not the periodic
// re-announce, so the timer never needs to fire.
class fake_timer
{
public:
    explicit fake_timer(int &)
    {
    }

    void expires_after(std::chrono::milliseconds)
    {
    }

    void async_wait(plexus::detail::move_only_function<void(std::error_code)>)
    {
    }

    void cancel()
    {
    }
};

struct fake_policy
{
    using executor_type = int &;
    using timer_type    = fake_timer;

    static void post(executor_type, plexus::detail::move_only_function<void()> fn)
    {
        fn();
    }
};

static_assert(plexus::stream::datagram_socket<fake_datagram_socket>);

// A virtual clock the cap tests advance by hand so the per-source rate window is exercised with no
// wall-clock sleep. The cap calls now() once per inbound, so a test sets g_now before each inject.
struct fake_clock
{
    using duration   = std::chrono::milliseconds;
    using time_point = std::chrono::time_point<fake_clock, duration>;

    static inline time_point g_now{};

    static time_point now()
    {
        return g_now;
    }

    static void advance(std::chrono::milliseconds by)
    {
        g_now += by;
    }

    static void reset()
    {
        g_now = time_point{};
    }
};

using discovery_under_test = plexus::native::multicast_discovery<fake_datagram_socket, fake_policy>;
using discovery_capped     = plexus::native::multicast_discovery<fake_datagram_socket, fake_policy, fake_clock>;

} // namespace multicast_discovery_fixture
