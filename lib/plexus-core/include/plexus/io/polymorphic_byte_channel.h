#ifndef HPP_GUARD_PLEXUS_IO_POLYMORPHIC_BYTE_CHANNEL_H
#define HPP_GUARD_PLEXUS_IO_POLYMORPHIC_BYTE_CHANNEL_H

#include "plexus/wire/stream_inbound.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/byte_channel.h"
#include "plexus/io/detail/drop_event.h"

#include "plexus/detail/compat.h"

#include <span>
#include <memory>
#include <utility>
#include <cstddef>
#include <cstdint>

namespace plexus::io {

// The type-erased channel a MULTI-transport Policy binds as its byte_channel_type.
// A single-transport node never instantiates it (asio_policy/unix_policy bind the
// concrete channel directly — zero indirection, the MCU profile). The erasure is a
// thin abstract base, NOT a std::variant of the gated concrete channels (forbidden):
// the concrete types are assembled behind the base in a backend-visible header,
// never enumerated.
//
// concrete_channel_base mirrors the seven byte_channel verbs (send, close,
// remote_endpoint [const], on_data, on_closed, on_error, on_protocol_close — on_closed
// is DISTINCT from on_protocol_close) PLUS one additive occupancy read, backpressured():
// it is NOT a byte_channel concept verb and NOT a wire/send change — it exposes the
// queued-byte occupancy the egress scheduler polls so the erased multi-transport path
// bands instead of always-accepting. channel_adapter<C> owns the real concrete
// channel and forwards each verb STRAIGHT THROUGH to it (the concrete channel keeps
// its own stored callbacks; the adapter stores none — load-bearing for the steady-
// state no-alloc gate). polymorphic_byte_channel holds one unique_ptr<concrete_channel_base>
// and forwards each verb through ONE virtual hop; the unique_ptr is minted ONCE at dial,
// so the steady-state publish loop allocates nothing. Making backpressured() PURE on the
// base forces every wrapped channel to expose it at compile time — no silent gap.

class concrete_channel_base
{
public:
    virtual ~concrete_channel_base() = default;

    virtual void send(std::span<const std::byte> data) = 0;
    virtual void close() = 0;
    [[nodiscard]] virtual endpoint remote_endpoint() const = 0;
    virtual void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)> cb) = 0;
    virtual void on_closed(plexus::detail::move_only_function<void()> cb) = 0;
    virtual void on_error(plexus::detail::move_only_function<void(io_error)> cb) = 0;
    virtual void on_protocol_close(plexus::detail::move_only_function<void(wire::close_cause)> cb) = 0;
    [[nodiscard]] virtual std::size_t backpressured() const = 0;
    [[nodiscard]] virtual std::uint64_t scheduler_key() const = 0;

    // The optional drop edge: a wrapped channel that surfaces unroutable/congested drops
    // (inproc, shm) forwards the engine's posted drop_sink down to its concrete on_drop;
    // a channel with no such edge swallows the install (the default), so the erasure keeps
    // on_drop OUT of the byte_channel concept while still threading it where it exists.
    virtual void on_drop(plexus::detail::move_only_function<void(const detail::drop_event &)>) {}
};

template <typename C>
class channel_adapter final : public concrete_channel_base
{
public:
    explicit channel_adapter(std::unique_ptr<C> c) : m_c(std::move(c)) {}

    void send(std::span<const std::byte> data) override { m_c->send(data); }
    void close() override { m_c->close(); }
    [[nodiscard]] endpoint remote_endpoint() const override { return m_c->remote_endpoint(); }
    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)> cb) override { m_c->on_data(std::move(cb)); }
    void on_closed(plexus::detail::move_only_function<void()> cb) override { m_c->on_closed(std::move(cb)); }
    void on_error(plexus::detail::move_only_function<void(io_error)> cb) override { m_c->on_error(std::move(cb)); }
    void on_protocol_close(plexus::detail::move_only_function<void(wire::close_cause)> cb) override { m_c->on_protocol_close(std::move(cb)); }
    [[nodiscard]] std::size_t backpressured() const override { return m_c->backpressured(); }
    [[nodiscard]] std::uint64_t scheduler_key() const override { return m_c->scheduler_key(); }

    // Forward the drop_sink install only to a concrete channel that has the edge; a channel
    // without on_drop leaves the base default (the install is dropped — that tier surfaces
    // no channel-level drop).
    void on_drop(plexus::detail::move_only_function<void(const detail::drop_event &)> cb) override
    {
        if constexpr(requires { m_c->on_drop(std::move(cb)); })
            m_c->on_drop(std::move(cb));
    }

private:
    std::unique_ptr<C> m_c;
};

class polymorphic_byte_channel
{
public:
    explicit polymorphic_byte_channel(std::unique_ptr<concrete_channel_base> impl) : m_impl(std::move(impl)) {}

    polymorphic_byte_channel(const polymorphic_byte_channel &) = delete;
    polymorphic_byte_channel &operator=(const polymorphic_byte_channel &) = delete;
    polymorphic_byte_channel(polymorphic_byte_channel &&) = default;
    polymorphic_byte_channel &operator=(polymorphic_byte_channel &&) = default;

    void send(std::span<const std::byte> data) { m_impl->send(data); }
    void close() { m_impl->close(); }
    [[nodiscard]] endpoint remote_endpoint() const { return m_impl->remote_endpoint(); }
    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)> cb) { m_impl->on_data(std::move(cb)); }
    void on_closed(plexus::detail::move_only_function<void()> cb) { m_impl->on_closed(std::move(cb)); }
    void on_error(plexus::detail::move_only_function<void(io_error)> cb) { m_impl->on_error(std::move(cb)); }
    void on_protocol_close(plexus::detail::move_only_function<void(wire::close_cause)> cb) { m_impl->on_protocol_close(std::move(cb)); }
    [[nodiscard]] std::size_t backpressured() const { return m_impl->backpressured(); }
    [[nodiscard]] std::uint64_t scheduler_key() const { return m_impl->scheduler_key(); }
    void on_drop(plexus::detail::move_only_function<void(const detail::drop_event &)> cb) { m_impl->on_drop(std::move(cb)); }

private:
    std::unique_ptr<concrete_channel_base> m_impl;
};

}

static_assert(plexus::io::byte_channel<plexus::io::polymorphic_byte_channel>,
    "polymorphic_byte_channel must satisfy byte_channel — check the seven erased verbs");

#endif
