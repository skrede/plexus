#ifndef HPP_GUARD_TESTS_INTEGRATION_SESSION_STALE_TAP_H
#define HPP_GUARD_TESTS_INTEGRATION_SESSION_STALE_TAP_H

#include "test_routing_engine_inproc_common.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/object_carrier.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/frame_codec.h"

#include "plexus/detail/compat.h"

#include <span>
#include <memory>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>

namespace session_stale_fixture {

using namespace routing_inproc_fixture;
using channel_t = inproc_channel<manual_clock>;

enum class mute : std::uint8_t
{
    none,
    all,
    heartbeats,
    unsubscribes
};

struct sent_frame
{
    manual_clock::time_point at;
    plexus::wire::msg_type type;
};

// Spliced between one side's channel and its partner, so everything that side sends passes here first: it records
// each frame and drops what the owner's mute mode silences.
class relay
{
public:
    relay(inproc_executor<manual_clock> &ex, channel_t &from, const mute &mode)
            : m_mode(mode)
            , m_channel(ex)
            , m_to_key(ex.bus().key_for(from.remote_endpoint()))
            , m_bus(ex.bus())
            , m_sent()
    {
        m_channel.connect_to(from.remote_endpoint());
        from.connect_to(m_channel.local_endpoint());
        m_channel.on_data([this](std::span<const std::byte> f) { forward(f); });
        m_channel.on_object([this](const plexus::io::object_carrier &c) { forward(c); });
        m_channel.on_error([this](plexus::io::io_error) { m_bus.enqueue_close(m_to_key); });
    }

    const std::vector<sent_frame> &sent() const noexcept
    {
        return m_sent;
    }

private:
    const mute &m_mode;
    channel_t m_channel;
    std::uint64_t m_to_key;
    inproc_bus<manual_clock> &m_bus;
    std::vector<sent_frame> m_sent;

    void forward(std::span<const std::byte> f)
    {
        const auto hdr  = plexus::wire::decode_header(f);
        const auto type = hdr ? hdr->type : plexus::wire::msg_type{};
        m_sent.push_back(sent_frame{manual_clock::now(), type});
        if(passes(type))
            m_channel.send(f);
    }

    bool passes(plexus::wire::msg_type type) const noexcept
    {
        switch(m_mode)
        {
            case mute::none:
                return true;
            case mute::all:
                return false;
            case mute::heartbeats:
                return type != plexus::wire::msg_type::heartbeat;
            case mute::unsubscribes:
                return type != plexus::wire::msg_type::unsubscribe;
        }
        return true;
    }

    void forward(const plexus::io::object_carrier &c)
    {
        if(m_mode != mute::all)
            m_channel.send_object(c);
        plexus::io::release(c);
    }
};

// An inproc transport that splices a relay into every channel it mints, so a test can silence this side mid-session.
class tapping_transport
{
public:
    using channel_cb = plexus::detail::move_only_function<void(std::unique_ptr<channel_t>)>;
    using dialed_cb  = plexus::detail::move_only_function<void(std::unique_ptr<channel_t>, const endpoint &)>;

    tapping_transport(inproc_executor<manual_clock> &ex, inproc_bus<manual_clock> &bus)
            : m_ex(ex)
            , m_mode(mute::none)
            , m_inner(ex, bus)
            , m_relays()
    {
    }

    void on_accepted(channel_cb cb)
    {
        m_inner.on_accepted([this, cb = std::move(cb)](std::unique_ptr<channel_t> ch) mutable { cb(splice(std::move(ch))); });
    }

    void on_dialed(dialed_cb cb)
    {
        m_inner.on_dialed([this, cb = std::move(cb)](std::unique_ptr<channel_t> ch, const endpoint &ep) mutable { cb(splice(std::move(ch)), ep); });
    }

    void on_dial_failed(plexus::detail::move_only_function<void(const endpoint &, plexus::io::io_error)> cb)
    {
        m_inner.on_dial_failed(std::move(cb));
    }

    void on_error(plexus::detail::move_only_function<void(plexus::io::io_error)> cb)
    {
        m_inner.on_error(std::move(cb));
    }

    void listen(const endpoint &ep)
    {
        m_inner.listen(ep);
    }

    void dial(const endpoint &ep)
    {
        m_inner.dial(ep);
    }

    void close()
    {
        m_inner.close();
    }

    void silence(mute m) noexcept
    {
        m_mode = m;
    }

    const relay &relay_at(std::size_t n) const
    {
        return *m_relays.at(n);
    }

    std::size_t relay_count() const noexcept
    {
        return m_relays.size();
    }

private:
    inproc_executor<manual_clock> &m_ex;
    mute m_mode;
    inproc_transport<manual_clock> m_inner;
    std::vector<std::unique_ptr<relay>> m_relays;

    std::unique_ptr<channel_t> splice(std::unique_ptr<channel_t> ch)
    {
        m_relays.push_back(std::make_unique<relay>(m_ex, *ch, m_mode));
        return ch;
    }
};

}

#endif
