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

// Whether a leaf exposes the same-host ring-acquire probe (the shared-memory member): it picks
// the per-leaf construction recipe — an shm member builds from {io, broker}, every plain leaf
// from {io}.
template<typename T>
concept has_can_acquire = requires(T &t) {
    { t.can_acquire(std::declval<const io::endpoint &>()) } -> std::convertible_to<bool>;
};

// The owning storage for a pack of NON-MOVABLE leaves (each plexus transport pins its address
// via `this`-capturing closures). A std::tuple cannot hold them — its element init requires
// move/copy-constructibility — so this recursive holder direct-initializes each member in place
// from a per-leaf factory (guaranteed copy elision), never constructed-then-moved.
template<typename... Ts>
struct leaf_storage;

template<>
struct leaf_storage<>
{
    template<typename Factory>
    explicit leaf_storage(Factory &&) noexcept
    {
    }
};

template<typename Head, typename... Rest>
struct leaf_storage<Head, Rest...>
{
    Head head;
    leaf_storage<Rest...> tail;

    template<typename Factory>
    explicit leaf_storage(Factory &&factory)
            : head(factory.template make<Head>())
            , tail(factory)
    {
    }
};

template<std::size_t I, typename Head, typename... Rest>
auto &leaf_at(leaf_storage<Head, Rest...> &storage) noexcept
{
    if constexpr(I == 0)
        return storage.head;
    else
        return leaf_at<I - 1>(storage.tail);
}

}

// An owning composition bundle: name the transports once, construct the leaves in place, and
// mint a node that borrows them. An shm leaf is supplied via plexus_make_set_leaf, found by ADL
// on the broker argument and declared behind the SEPARATE shm-include root, so transport_set.h
// stays shm-free (a non-shm set drags in no shm/crypto headers).
//
// LIFETIME: the set OWNS the leaves and BORROWS the io_context (and broker). make_node returns a
// node that BORROWS those leaves, so the transport_set MUST OUTLIVE every node minted from it
// (and the io_context/broker must outlive the set). Non-copyable / non-movable.
template<typename... Transports>
    requires(sizeof...(Transports) >= 1)
class transport_set
{
public:
    static constexpr bool has_shm_member = (detail::has_can_acquire<Transports> || ...);

    explicit transport_set(::asio::io_context &io)
        requires(!has_shm_member)
            : m_io(io)
            , m_storage(no_broker_factory{io})
    {
    }

    // region_ns is the static shm-region namespace forwarded to the shm leaf: empty (the
    // default) shares rings by topic, a distinct namespace isolates this set's same-host shm
    // from unrelated co-host apps.
    template<typename Broker>
    transport_set(::asio::io_context &io, Broker &broker, std::string region_ns = {})
        requires(has_shm_member)
            : m_io(io)
            , m_storage(broker_factory<Broker>{io, broker, std::move(region_ns)})
    {
    }

    transport_set(const transport_set &)            = delete;
    transport_set &operator=(const transport_set &) = delete;
    transport_set(transport_set &&)                 = delete;
    transport_set &operator=(transport_set &&)      = delete;

    template<typename Policy>
    node<Policy, Transports...> make_node(discovery::discovery &disc, const plexus::node_id &id, const node_options &opts)
    {
        return make_node_impl<Policy>(disc, id, opts, std::index_sequence_for<Transports...>{});
    }

private:
    struct no_broker_factory
    {
        ::asio::io_context &io;
        template<typename T>
        T make()
        {
            return T{io};
        }
    };

    template<typename Broker>
    struct broker_factory
    {
        ::asio::io_context &io;
        Broker &broker;
        std::string region_ns;
        template<typename T>
        T make()
        {
            if constexpr(detail::has_can_acquire<T>)
                return plexus_make_set_leaf(std::type_identity<T>{}, io, broker, region_ns);
            else
                return T{io};
        }
    };

    template<typename Policy, std::size_t... Is>
    node<Policy, Transports...> make_node_impl(discovery::discovery &disc, const plexus::node_id &id, const node_options &opts, std::index_sequence<Is...>)
    {
        return node<Policy, Transports...>{m_io, disc, id, detail::leaf_at<Is>(m_storage)..., opts};
    }

    ::asio::io_context &m_io;
    detail::leaf_storage<Transports...> m_storage;
};

}

#endif
