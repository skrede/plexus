#ifndef HPP_GUARD_PLEXUS_ASIO_TRANSPORT_SET_H
#define HPP_GUARD_PLEXUS_ASIO_TRANSPORT_SET_H

#include "plexus/node.h"
#include "plexus/node_id.h"
#include "plexus/node_options.h"

#include "plexus/discovery/discovery.h"

#include <asio/io_context.hpp>

#include <string>
#include <utility>
#include <concepts>
#include <type_traits>

namespace plexus::asio {

namespace detail {

// Whether a leaf exposes the same-host ring-acquire probe (the shared-memory member). The
// SAME discriminator node.h uses to decide its same-host preference hook — here it picks
// the per-leaf construction recipe: an shm member builds from {io, broker}, every plain
// stream/datagram leaf builds from {io}.
template <typename T>
concept has_can_acquire =
    requires(T &t) { { t.can_acquire(std::declval<const io::endpoint &>()) } -> std::convertible_to<bool>; };

// The owning storage for a pack of NON-MOVABLE leaves: every plexus transport deletes its
// copy and declares no move (the stream base captures `this` in its accept/dial closures, so
// it pins its address). A std::tuple cannot hold them — its element init requires
// move/copy-constructibility. This recursive holder direct-initializes each member in place
// from a per-leaf factory (guaranteed copy elision turns the factory's prvalue into the
// member), so a leaf is constructed, never constructed-then-moved.
template <typename... Ts>
struct leaf_storage;

template <>
struct leaf_storage<>
{
    template <typename Factory>
    explicit leaf_storage(Factory &&) noexcept
    {
    }
};

template <typename Head, typename... Rest>
struct leaf_storage<Head, Rest...>
{
    Head head;
    leaf_storage<Rest...> tail;

    template <typename Factory>
    explicit leaf_storage(Factory &&factory)
        : head(factory.template make<Head>())
        , tail(factory)
    {
    }
};

template <std::size_t I, typename Head, typename... Rest>
auto &leaf_at(leaf_storage<Head, Rest...> &storage) noexcept
{
    if constexpr(I == 0)
        return storage.head;
    else
        return leaf_at<I - 1>(storage.tail);
}

}

// The per-leaf factory ADL customization point keyed on the broker namespace: a plain leaf
// is built by the transport_set itself ({io}); an shm leaf needs make_shm_member(io, broker),
// which lives behind the SEPARATE shm-include root. So transport_set.h stays shm-free (a
// non-shm set drags in no shm/crypto headers) and the shm-bearing build supplies the leaf via
// plexus_make_set_leaf, found by ADL on the broker argument. The shm header declares the
// overload (plexus/asio/shm/linux/shm_member.h); a pack with no shm leaf never names it.

// An owning composition bundle: name the transports once, construct the leaves in place, and
// mint a node that borrows them — replacing the hand-built-leaf-plus-node boilerplate. A pack
// WITH an shm member is built from {io, broker}; a pack with none from {io}; the wrong ctor is
// a compile error (the requires-clauses below).
//
// LIFETIME: the set OWNS the leaves and BORROWS the io_context (and broker). make_node returns
// a node that BORROWS those leaves, so the transport_set MUST OUTLIVE every node minted from
// it (and the io_context/broker must outlive the set) — the same single-owner teardown
// discipline node.h documents. Non-copyable / non-movable: the borrowed node leaves pin the
// set's address.
template <typename... Transports>
    requires(sizeof...(Transports) >= 1)
class transport_set
{
public:
    static constexpr bool has_shm_member = (detail::has_can_acquire<Transports> || ...);

    // The no-shm ctor: every leaf builds from {io}. Gated off a pack that carries an shm
    // member — that pack needs the broker ctor below, and selecting this one is a clear error.
    explicit transport_set(::asio::io_context &io)
        requires(!has_shm_member)
        : m_io(io), m_storage(no_broker_factory{io})
    {
    }

    // The shm-bearing ctor: the shm leaf builds from {io, broker, region_ns}, every other from
    // {io}. Gated on a pack that actually carries an shm member, so a no-shm pack cannot pass a
    // stray broker. region_ns is the static shm-region namespace forwarded to the shm leaf:
    // empty (the default) shares rings by topic, a distinct namespace isolates this set's
    // same-host shm from unrelated co-host apps.
    template <typename Broker>
    transport_set(::asio::io_context &io, Broker &broker, std::string region_ns = {})
        requires(has_shm_member)
        : m_io(io), m_storage(broker_factory<Broker>{io, broker, std::move(region_ns)})
    {
    }

    transport_set(const transport_set &) = delete;
    transport_set &operator=(const transport_set &) = delete;
    transport_set(transport_set &&) = delete;
    transport_set &operator=(transport_set &&) = delete;

    // Mint a node over the owned leaves. The leaves expand into the borrowing node ctor in
    // pack order; the node is returned as a prvalue, so guaranteed copy elision materializes
    // the non-movable node directly in the caller's storage (no move is required or attempted).
    template <typename Policy>
    [[nodiscard]] node<Policy, Transports...> make_node(discovery::discovery &disc,
                                                        const plexus::node_id &id,
                                                        const node_options &opts)
    {
        return make_node_impl<Policy>(disc, id, opts, std::index_sequence_for<Transports...>{});
    }

private:
    struct no_broker_factory
    {
        ::asio::io_context &io;
        template <typename T>
        T make()
        {
            return T{io};
        }
    };

    template <typename Broker>
    struct broker_factory
    {
        ::asio::io_context &io;
        Broker &broker;
        std::string region_ns;
        template <typename T>
        T make()
        {
            if constexpr(detail::has_can_acquire<T>)
                return plexus_make_set_leaf(std::type_identity<T>{}, io, broker, region_ns);
            else
                return T{io};
        }
    };

    template <typename Policy, std::size_t... Is>
    node<Policy, Transports...> make_node_impl(discovery::discovery &disc, const plexus::node_id &id,
                                               const node_options &opts, std::index_sequence<Is...>)
    {
        return node<Policy, Transports...>{m_io, disc, id,
                                           detail::leaf_at<Is>(m_storage)..., opts};
    }

    ::asio::io_context &m_io;
    detail::leaf_storage<Transports...> m_storage;
};

}

#endif
