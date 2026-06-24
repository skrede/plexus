#ifndef HPP_GUARD_PLEXUS_IO_MULTIPLEXING_TRANSPORT_H
#define HPP_GUARD_PLEXUS_IO_MULTIPLEXING_TRANSPORT_H

#include "plexus/detail/compat.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/transport_backend.h"
#include "plexus/io/transport_selector.h"
#include "plexus/io/polymorphic_byte_channel.h"

#include <span>
#include <array>
#include <tuple>
#include <memory>
#include <cstddef>
#include <utility>
#include <string_view>
#include <type_traits>

namespace plexus::io {

// A member transport composed into a multiplexer: it presents the typed completion setters against
// its OWN concrete channel (M::channel_type — NOT the erased polymorphic_byte_channel, so it does
// NOT satisfy transport_backend against the multiplexer's policy) plus listen/dial/close, and
// advertises the schemes it serves and its locality tier so the multiplexer routes over the pack.
template<typename M>
concept mux_member = requires(M &m, const endpoint &ep, plexus::detail::move_only_function<void(std::unique_ptr<typename M::channel_type>)> on_acc,
                              plexus::detail::move_only_function<void(std::unique_ptr<typename M::channel_type>, const endpoint &)> on_ch,
                              plexus::detail::move_only_function<void(const endpoint &, io_error)> on_dfail, plexus::detail::move_only_function<void(io_error)> on_err) {
    { m.listen(ep) } -> std::same_as<void>;
    m.on_accepted(std::move(on_acc));
    { m.dial(ep) } -> std::same_as<void>;
    m.on_dialed(std::move(on_ch));
    m.on_dial_failed(std::move(on_dfail));
    m.on_error(std::move(on_err));
    { m.close() } -> std::same_as<void>;
    { M::mux_schemes } -> std::convertible_to<std::span<const std::string_view>>;
    { M::mux_tier } -> std::convertible_to<transport_kind>;
};

// A per-candidate eligibility descriptor the selection hook reads to pick one member when a tier
// resolves to N candidates: the member index + whether this candidate is the same-host fast-path.
struct mux_candidate
{
    std::size_t index        = 0;
    bool local_fast_eligible = false;
};

// Whether a member opts into the same-host fast path via `static constexpr bool
// mux_prefers_local_fast = true`. A trait, not a concept requirement, so every member stays valid.
template<typename M, typename = void>
struct member_prefers_local_fast : std::false_type
{
};

template<typename M>
struct member_prefers_local_fast<M, std::void_t<decltype(M::mux_prefers_local_fast)>> : std::bool_constant<M::mux_prefers_local_fast>
{
};

// The selection hook's erased type: when a tier resolves to N candidate members, the hook picks one
// by index over a span of per-candidate descriptors — NOT the members tuple, so it stays decoupled
// from the concrete member types. A cold-path dial/listen call, never the hot loop.
using selection_hook = plexus::detail::move_only_function<std::size_t(const endpoint &, std::span<const mux_candidate>)>;

struct first_candidate
{
    std::size_t operator()(const endpoint &, std::span<const mux_candidate> candidates) const noexcept
    {
        return candidates.front().index;
    }
};

}

// Included after mux_candidate / member_prefers_local_fast are defined and before the class that
// befriends and calls it, to avoid a circular include.
#include "plexus/io/detail/mux_dispatch.h"

namespace plexus::io {

// The variadic multiplexing transport_backend: it BORROWS a pack of member transports + a
// transport_selector + a selection hook, and presents the single erased transport_backend surface
// the engine drives unchanged. On dial/listen it resolves ONE member by scheme over the pack
// (locality wins first). The borrowed members MUST outlive this object; the owner sequences teardown.
template<mux_member... Members>
class multiplexing_transport
{
public:
    template<std::size_t I>
    using member_type = std::remove_reference_t<std::tuple_element_t<I, std::tuple<Members...>>>;
    template<typename M>
    static constexpr bool member_prefers_local_fast_v = member_prefers_local_fast<M>::value;

    explicit multiplexing_transport(Members &...members, transport_selector selector = {}, selection_hook hook = first_candidate{})
            : m_selector(selector)
            , m_members(members...)
            , m_hook(std::move(hook))
    {
        std::apply([this](auto &...m) { (detail::wire_member(*this, m), ...); }, m_members);
    }

    multiplexing_transport(const multiplexing_transport &)            = delete;
    multiplexing_transport &operator=(const multiplexing_transport &) = delete;

    void on_accepted(plexus::detail::move_only_function<void(std::unique_ptr<polymorphic_byte_channel>)> cb)
    {
        m_on_accepted = std::move(cb);
    }

    void on_dialed(plexus::detail::move_only_function<void(std::unique_ptr<polymorphic_byte_channel>, const endpoint &)> cb)
    {
        m_on_dialed = std::move(cb);
    }

    void on_dial_failed(plexus::detail::move_only_function<void(const endpoint &, io_error)> cb)
    {
        m_on_dial_failed = std::move(cb);
    }

    void on_error(plexus::detail::move_only_function<void(io_error)> cb)
    {
        m_on_error = std::move(cb);
    }

    void listen(const endpoint &ep)
    {
        dispatch(route_of(ep), [&](auto &m) { m.listen(ep); });
    }

    void dial(const endpoint &ep)
    {
        dispatch(route_of(ep), [&](auto &m) { m.dial(ep); });
    }

    void close()
    {
        std::apply([](auto &...m) { (m.close(), ...); }, m_members);
    }

    // Apply fn to each borrowed member by reference (cold-path): the caller selects the member it
    // cares about with an if-constexpr capability check, so a member lacking the verb is a no-op.
    template<typename F>
    void for_each_member(F &&fn)
    {
        std::apply([&](auto &...m) { (fn(m), ...); }, m_members);
    }

private:
    transport_selector m_selector;
    std::tuple<Members &...> m_members;
    selection_hook m_hook;
    plexus::detail::move_only_function<void(io_error)> m_on_error;
    plexus::detail::move_only_function<void(const endpoint &, io_error)> m_on_dial_failed;
    plexus::detail::move_only_function<void(std::unique_ptr<polymorphic_byte_channel>)> m_on_accepted;
    plexus::detail::move_only_function<void(std::unique_ptr<polymorphic_byte_channel>, const endpoint &)> m_on_dialed;

    template<typename Mux, typename M>
    friend void detail::wire_member(Mux &, M &);
    template<typename Mux, std::size_t I, typename Candidates>
    friend void detail::mux_consider(const Mux &, const endpoint &, transport_kind, Candidates &, std::size_t &);

    // The resolved member tier for an endpoint: locality wins first, so a remote member never
    // serves a same-host-confined peer even though it rides the remote tier.
    transport_kind tier_of(const endpoint &ep) const noexcept
    {
        return m_selector.select(ep, m_selector.reliability_of_scheme(ep.scheme));
    }

    // Search the pack for the members serving ep.scheme within its tier, then let the selection hook
    // pick one. A scheme no member advertises in its tier resolves to no candidate (dispatch no-ops).
    std::size_t route_of(const endpoint &ep) noexcept
    {
        const transport_kind tier = tier_of(ep);
        std::array<mux_candidate, sizeof...(Members)> candidates{};
        std::size_t count = 0;
        detail::mux_collect_candidates(*this, ep, tier, candidates, count, std::index_sequence_for<Members...>{});
        if(count == 0)
            return sizeof...(Members);
        return m_hook(ep, {candidates.data(), count});
    }

    template<typename F>
    void dispatch(std::size_t index, F &&fn)
    {
        dispatch_at(index, std::forward<F>(fn), std::index_sequence_for<Members...>{});
    }

    template<typename F, std::size_t... I>
    void dispatch_at(std::size_t index, F &&fn, std::index_sequence<I...>)
    {
        ((index == I ? (void)fn(std::get<I>(m_members)) : (void)0), ...);
    }
};

}

#endif
