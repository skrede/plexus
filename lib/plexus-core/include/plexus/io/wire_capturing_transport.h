#ifndef HPP_GUARD_PLEXUS_IO_WIRE_CAPTURING_TRANSPORT_H
#define HPP_GUARD_PLEXUS_IO_WIRE_CAPTURING_TRANSPORT_H

#include "plexus/policy.h"

#include "plexus/detail/compat.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/recording_channel.h"
#include "plexus/io/transport_backend.h"

#include <memory>
#include <utility>

namespace plexus::io {

// An Inner policy with its byte_channel_type swapped for the recording_channel decorator: the
// engine over this policy mints recording_channel channels, so the wire tier is STRUCTURALLY
// present by TYPE, fixed at construction (no runtime branch).
template<typename Inner>
struct wire_capturing_policy
{
    using executor_type     = typename Inner::executor_type;
    using byte_channel_type = recording_channel<typename Inner::byte_channel_type>;
    using timer_type        = typename Inner::timer_type;
    using byte_owner        = typename Inner::byte_owner;

    static void post(executor_type ex, plexus::detail::move_only_function<void()> fn)
    {
        Inner::post(ex, std::move(fn));
    }
};

// Drives a borrowed Inner transport and, at the on_dialed / on_accepted mint point, MOVES the
// bare channel into a recording_channel and forwards the decorated channel up.
template<typename InnerTransport, typename Inner>
class wire_capturing_transport
{
public:
    using inner_channel_type = typename Inner::byte_channel_type;
    using channel_type       = recording_channel<inner_channel_type>;

    explicit wire_capturing_transport(InnerTransport &inner)
            : m_inner(inner)
    {
    }

    wire_capturing_transport(const wire_capturing_transport &)            = delete;
    wire_capturing_transport &operator=(const wire_capturing_transport &) = delete;

    void on_accepted(plexus::detail::move_only_function<void(std::unique_ptr<channel_type>)> cb)
    {
        m_on_accepted = std::move(cb);
        m_inner.on_accepted(
                [this](std::unique_ptr<inner_channel_type> ch)
                {
                    if(m_on_accepted)
                        m_on_accepted(std::make_unique<channel_type>(std::move(ch)));
                });
    }

    void on_dialed(plexus::detail::move_only_function<void(std::unique_ptr<channel_type>, const endpoint &)> cb)
    {
        m_on_dialed = std::move(cb);
        m_inner.on_dialed(
                [this](std::unique_ptr<inner_channel_type> ch, const endpoint &ep)
                {
                    if(m_on_dialed)
                        m_on_dialed(std::make_unique<channel_type>(std::move(ch)), ep);
                });
    }

    void on_dial_failed(plexus::detail::move_only_function<void(const endpoint &, io_error)> cb)
    {
        m_inner.on_dial_failed(std::move(cb));
    }

    void on_error(plexus::detail::move_only_function<void(io_error)> cb)
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

private:
    InnerTransport &m_inner;
    plexus::detail::move_only_function<void(std::unique_ptr<channel_type>)> m_on_accepted;
    plexus::detail::move_only_function<void(std::unique_ptr<channel_type>, const endpoint &)> m_on_dialed;
};

}

#endif
