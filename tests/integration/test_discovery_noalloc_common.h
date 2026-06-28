#ifndef HPP_GUARD_TESTS_INTEGRATION_DISCOVERY_NOALLOC_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_DISCOVERY_NOALLOC_COMMON_H

#include "plexus/node_id.h"
#include "plexus/discovery/contact_card.h"
#include "plexus/discovery/discovery.h"
#include "plexus/stream/datagram_socket.h"

#include "plexus/detail/compat.h"

#include <span>
#include <chrono>
#include <memory>
#include <vector>
#include <string>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <system_error>

namespace discovery_noalloc_fixture {

struct sink_timer;

struct sink_executor
{
    sink_timer *armed{nullptr};
};

// A controllable timer: async_wait records the re-announce callback into a reused move_only_fn
// slot (a single this-capture, so it lives in the SBO and never allocates), and fire() invokes it
// to drive one production re-announce. arm_timer re-installs the same small callback each tick, so
// the steady-state re-announce path stores it without heap churn. The timer registers itself on
// the executor so the test can drive the discovery's internally-owned timer.
struct sink_timer
{
    explicit sink_timer(sink_executor &ex)
    {
        ex.armed = this;
    }
    void expires_after(std::chrono::milliseconds)
    {
    }
    void async_wait(plexus::detail::move_only_function<void(std::error_code)> cb)
    {
        m_cb = std::move(cb);
    }
    void cancel()
    {
        m_cb = nullptr;
    }
    void fire()
    {
        if(m_cb)
            m_cb(std::error_code{});
    }

    plexus::detail::move_only_function<void(std::error_code)> m_cb;
};

struct sink_policy
{
    using executor_type = sink_executor &;
    using timer_type    = sink_timer;
    using byte_owner    = std::shared_ptr<const void>;

    static void post(executor_type, plexus::detail::move_only_function<void()> fn)
    {
        fn();
    }
};

// A non-allocating datagram_socket: send_multicast records the emitted bytes into a reused buffer
// (no per-emit alloc once warm, mirroring the inproc sink_channel), and captured on_datagram lets
// the test replay a recorded announcement straight into the discovery decode path. endpoint_type
// is a minimal value carrying the source host string the decode path reads back.
class sink_datagram_socket
{
public:
    struct address_view
    {
        std::string host;
        std::string to_string() const
        {
            return host;
        }
    };
    struct endpoint_type
    {
        std::string host;
        void port(std::uint16_t)
        {
        }
        address_view address() const
        {
            return {host};
        }
    };

    std::error_code bind(const endpoint_type &)
    {
        return {};
    }

    void send_multicast(std::span<const std::byte> bytes)
    {
        last.assign(bytes.begin(), bytes.end());
        ++sends;
    }

    void on_datagram(plexus::detail::move_only_function<void(const endpoint_type &, std::span<const std::byte>)> cb)
    {
        m_on_datagram = std::move(cb);
    }

    void close()
    {
    }

    void replay(const std::string &source_host)
    {
        if(m_on_datagram)
            m_on_datagram(endpoint_type{source_host}, last);
    }

    std::vector<std::byte> last;
    int sends{0};

private:
    plexus::detail::move_only_function<void(const endpoint_type &, std::span<const std::byte>)> m_on_datagram;
};

static_assert(plexus::stream::datagram_socket<sink_datagram_socket>);

inline plexus::discovery::service_info make_card()
{
    plexus::node_id id{};
    id[0]  = std::byte{0x11};
    id[15] = std::byte{0xb4};
    const auto hex = plexus::discovery::detail::hex_encode(id);
    return {hex, {}, plexus::discovery::assemble_contact_card(id, {{"tcp", 5555}})};
}

}

#endif
