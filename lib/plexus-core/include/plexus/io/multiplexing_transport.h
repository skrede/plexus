#ifndef HPP_GUARD_PLEXUS_IO_MULTIPLEXING_TRANSPORT_H
#define HPP_GUARD_PLEXUS_IO_MULTIPLEXING_TRANSPORT_H

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/transport_backend.h"
#include "plexus/io/transport_selector.h"
#include "plexus/io/polymorphic_byte_channel.h"
#include "plexus/detail/compat.h"

#include <span>
#include <tuple>
#include <array>
#include <memory>
#include <cstddef>
#include <utility>
#include <type_traits>
#include <string_view>

namespace plexus::io {

// A member transport composed into a multiplexer: it presents the typed completion
// setters against its OWN concrete channel (M::channel_type — NOT the erased
// polymorphic_byte_channel; each member's byte_channel_type is its concrete channel,
// so it does NOT satisfy transport_backend against the multiplexer's policy) plus
// listen/dial/close, and advertises the schemes it serves and its locality tier so
// the multiplexer routes over the pack generically at compile time.
template<typename M>
concept mux_member = requires(
        M &m, const endpoint &ep,
        plexus::detail::move_only_function<void(std::unique_ptr<typename M::channel_type>)> on_acc,
        plexus::detail::move_only_function<void(std::unique_ptr<typename M::channel_type>,
                                                const endpoint &)>
                                                                             on_ch,
        plexus::detail::move_only_function<void(const endpoint &, io_error)> on_dfail,
        plexus::detail::move_only_function<void(io_error)>                   on_err) {
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

// A per-candidate eligibility descriptor: the value the selection hook reads to pick
// one member when a tier resolves to N candidates. It carries the candidate's member
// index plus a small static eligibility flag — whether this candidate is a same-host
// fast-path (shared-memory) member. The flag is a COMPILE-TIME property of the member
// type (mux_prefers_shm below), NOT a runtime acquire result: a preference hook reads
// it to know WHICH candidate is the fast path, then decides at acquire time whether to
// take it. The descriptor is a small VALUE — the hook receives a span of these, never
// the members tuple, so the erased signature stays decoupled from the concrete member
// types (the seed constraint).
struct mux_candidate
{
    std::size_t index        = 0;
    bool        shm_eligible = false;
};

// Whether a member is the same-host shared-memory fast-path candidate. Defaults false;
// a member opts in with `static constexpr bool mux_prefers_shm = true`. A trait (not a
// concept requirement) so every existing member stays a valid mux_member unchanged.
template<typename M, typename = void>
struct member_prefers_shm : std::false_type
{
};

template<typename M>
struct member_prefers_shm<M, std::void_t<decltype(M::mux_prefers_shm)>>
        : std::bool_constant<M::mux_prefers_shm>
{
};

// The selection hook's erased type: when a tier resolves to N candidate members, the
// hook picks one by index. It is a cold-path call (dial/listen, never the steady-state
// message loop), so the type erasure costs an indirection that never touches the hot
// path; the default below is empty, so it stays in SBO with no allocation. The hook
// receives the endpoint and a span of per-candidate eligibility descriptors (the index
// plus the same-host fast-path flag) — NOT the members tuple, so it stays erasable and
// decoupled from the concrete member types. A same-host-preference hook reads the
// shm_eligible flag to find the fast-path candidate and decides at acquire time whether
// to take it or fall back; the default below ignores the flag.
using selection_hook = plexus::detail::move_only_function<std::size_t(
        const endpoint &, std::span<const mux_candidate>)>;

// The default hook: return the first candidate, so today's single-candidate tiers behave
// identically. It is a small empty callable injected at construction (never a setter).
struct first_candidate
{
    [[nodiscard]] std::size_t operator()(const endpoint &,
                                         std::span<const mux_candidate> candidates) const noexcept
    {
        return candidates.front().index;
    }
};

// The variadic multiplexing transport_backend: it BORROWS a pack of member transports
// (held by reference) plus a transport_selector and a selection hook, and presents the
// single erased transport_backend surface (the polymorphic_byte_channel completions)
// the engine drives unchanged. The ctor installs this-capturing completion callbacks on
// each member that wrap<C> the member's concrete channel and re-emit it with the SAME
// endpoint. On dial/listen it resolves ONE member by scheme over the pack — locality
// wins first, so an encrypted remote member never serves a same-host-confined peer.
//
// LIFETIME: the ctor installs this-capturing completion callbacks into the borrowed
// members. The borrowed members MUST outlive this object — the owner sequences teardown
// so the multiplexer is destroyed before any member fires a late completion; no shared
// lifetime handle is taken. The multiplexer only BORROWS the members; it does not mint or
// own any credential the secure members carry.
template<mux_member... Members>
class multiplexing_transport
{
public:
    explicit multiplexing_transport(Members &...members, transport_selector selector = {},
                                    selection_hook hook = first_candidate{})
            : m_members(members...)
            , m_selector(selector)
            , m_hook(std::move(hook))
    {
        std::apply([this](auto &...m) { (wire_member(m), ...); }, m_members);
    }

    multiplexing_transport(const multiplexing_transport &)            = delete;
    multiplexing_transport &operator=(const multiplexing_transport &) = delete;

    void on_accepted(
            plexus::detail::move_only_function<void(std::unique_ptr<polymorphic_byte_channel>)> cb)
    {
        m_on_accepted = std::move(cb);
    }
    void on_dialed(plexus::detail::move_only_function<
                   void(std::unique_ptr<polymorphic_byte_channel>, const endpoint &)>
                           cb)
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

    // Apply fn to each borrowed member by reference (cold-path, dial/declare-time): the
    // caller selects the member it cares about with an if-constexpr capability check on
    // the member type, so a member that lacks the targeted verb is a no-op. Used by the
    // node's declare path to provision the same-host ring geometry on the shm member
    // without coupling to the member pack's order or concrete types.
    template<typename F>
    void for_each_member(F &&fn)
    {
        std::apply([&](auto &...m) { (fn(m), ...); }, m_members);
    }

private:
    template<typename C>
    static std::unique_ptr<polymorphic_byte_channel> wrap(std::unique_ptr<C> ch)
    {
        return std::make_unique<polymorphic_byte_channel>(
                std::make_unique<channel_adapter<C>>(std::move(ch)));
    }

    template<typename M>
    void wire_member(M &m)
    {
        using C = typename M::channel_type;
        m.on_accepted(
                [this](std::unique_ptr<C> ch)
                {
                    if(m_on_accepted)
                        m_on_accepted(wrap(std::move(ch)));
                });
        m.on_dialed(
                [this](std::unique_ptr<C> ch, const endpoint &ep)
                {
                    if(m_on_dialed)
                        m_on_dialed(wrap(std::move(ch)), ep);
                });
        m.on_dial_failed(
                [this](const endpoint &ep, io_error e)
                {
                    if(m_on_dial_failed)
                        m_on_dial_failed(ep, e);
                });
        m.on_error(
                [this](io_error e)
                {
                    if(m_on_error)
                        m_on_error(e);
                });
    }

    // The resolved member tier for an endpoint: locality wins first (same-host -> the
    // local member), so a remote member — including the encrypted ones — never serves a
    // same-host-confined peer even though it rides the remote tier.
    [[nodiscard]] transport_kind tier_of(const endpoint &ep) const noexcept
    {
        // The reliability axis reaches select() through dial(ep) by feeding it the path's
        // own class (the scheme is the only routing discriminator the engine path carries),
        // so the hint param is dial-reachable rather than a dead hardcoded constant. The
        // tier result is unchanged — select() classifies locality hint-neutrally.
        return m_selector.select(ep, m_selector.reliability_of_scheme(ep.scheme));
    }

    // Search the pack for the members serving ep.scheme within its tier, then let the
    // selection hook pick one. A scheme no member advertises in its tier resolves to no
    // candidate; the dispatch is then a no-op (the caller cannot form that path for a
    // composition that omits the member).
    [[nodiscard]] std::size_t route_of(const endpoint &ep) noexcept
    {
        const transport_kind                          tier = tier_of(ep);
        std::array<mux_candidate, sizeof...(Members)> candidates{};
        std::size_t                                   count = 0;
        collect_candidates(ep, tier, candidates, count, std::index_sequence_for<Members...>{});
        if(count == 0)
            return sizeof...(Members);
        return m_hook(ep, {candidates.data(), count});
    }

    template<std::size_t... I>
    void collect_candidates(const endpoint &ep, transport_kind tier,
                            std::array<mux_candidate, sizeof...(Members)> &out, std::size_t &count,
                            std::index_sequence<I...>) const noexcept
    {
        (consider<I>(ep, tier, out, count), ...);
    }

    template<std::size_t I>
    void consider(const endpoint &ep, transport_kind tier,
                  std::array<mux_candidate, sizeof...(Members)> &out,
                  std::size_t                                   &count) const noexcept
    {
        using M = std::remove_reference_t<std::tuple_element_t<I, std::tuple<Members...>>>;
        if(M::mux_tier != tier)
            return;
        for(std::string_view scheme : M::mux_schemes)
            if(scheme == ep.scheme)
            {
                out[count++] = mux_candidate{I, member_prefers_shm<M>::value};
                return;
            }
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

    std::tuple<Members &...> m_members;
    transport_selector       m_selector;
    selection_hook           m_hook;
    plexus::detail::move_only_function<void(std::unique_ptr<polymorphic_byte_channel>)>
            m_on_accepted;
    plexus::detail::move_only_function<void(std::unique_ptr<polymorphic_byte_channel>,
                                            const endpoint &)>
                                                                         m_on_dialed;
    plexus::detail::move_only_function<void(const endpoint &, io_error)> m_on_dial_failed;
    plexus::detail::move_only_function<void(io_error)>                   m_on_error;
};

}

#endif
