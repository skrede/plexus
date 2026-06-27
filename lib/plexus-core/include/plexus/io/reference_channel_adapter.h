#ifndef HPP_GUARD_PLEXUS_IO_REFERENCE_CHANNEL_ADAPTER_H
#define HPP_GUARD_PLEXUS_IO_REFERENCE_CHANNEL_ADAPTER_H

#include "plexus/io/io_error.h"
#include "plexus/io/endpoint.h"
#include "plexus/io/polymorphic_byte_channel.h"

#include "plexus/wire/close_cause.h"

#include "plexus/detail/compat.h"

#include <span>
#include <cstddef>
#include <cstdint>
#include <utility>

namespace plexus::io {

// Erases a concrete channel the wrapper does NOT own: the holder keeps the channel alive elsewhere
// (a node member, not a dial-minted unique_ptr) and hands a reference in. Used for the self-route on
// a multi-transport node, where the node owns the loopback channel directly and only needs it erased
// to the registry's polymorphic_byte_channel — the concrete keeps its own stored callbacks, the
// adapter forwards straight through, same as channel_adapter. The referenced channel carries no
// occupancy verbs (the self-channel never backpressures), so backpressured/scheduler_key are inert.
template<typename C>
class reference_channel_adapter final : public concrete_channel_base
{
public:
    explicit reference_channel_adapter(C &c)
            : m_c(c)
    {
    }

    void send(std::span<const std::byte> data) override
    {
        m_c.send(data);
    }
    void close() override
    {
        m_c.close();
    }
    endpoint remote_endpoint() const override
    {
        return m_c.remote_endpoint();
    }
    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)> cb) override
    {
        m_c.on_data(std::move(cb));
    }
    void on_closed(plexus::detail::move_only_function<void()> cb) override
    {
        m_c.on_closed(std::move(cb));
    }
    void on_error(plexus::detail::move_only_function<void(io_error)> cb) override
    {
        m_c.on_error(std::move(cb));
    }
    void on_protocol_close(plexus::detail::move_only_function<void(wire::close_cause)> cb) override
    {
        m_c.on_protocol_close(std::move(cb));
    }
    std::size_t backpressured() const override
    {
        return 0;
    }
    std::uint64_t scheduler_key() const override
    {
        return reinterpret_cast<std::uint64_t>(&m_c);
    }

private:
    C &m_c;
};

}

#endif
