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
#include <string_view>

namespace plexus::io {

// A member transport composed into a multiplexer: it presents the typed completion
// setters against its OWN concrete channel (M::channel_type — NOT the erased
// polymorphic_byte_channel; each member's byte_channel_type is its concrete channel,
// so it does NOT satisfy transport_backend against the multiplexer's policy) plus
// listen/dial/close, and advertises the schemes it serves and its locality tier so
// the multiplexer routes over the pack generically at compile time.
template <typename M>
concept mux_member = requires(M &m, const endpoint &ep,
        plexus::detail::move_only_function<void(std::unique_ptr<typename M::channel_type>)> on_acc,
        plexus::detail::move_only_function<void(std::unique_ptr<typename M::channel_type>, const endpoint &)> on_ch,
        plexus::detail::move_only_function<void(const endpoint &, io_error)> on_dfail,
        plexus::detail::move_only_function<void(io_error)> on_err)
{
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

// The selection hook's erased type: when a tier resolves to N candidate members, the
// hook picks one by index. It is a cold-path call (dial/listen, never the steady-state
// message loop), so the type erasure costs an indirection that never touches the hot
// path; the default below is empty, so it stays in SBO with no allocation. The hook
// receives the endpoint and the candidate indices — NOT the members tuple: a candidate-
// metadata-carrying signature is deferred to the same-host shared-memory member, which
// will finalize whether its same-host preference needs per-candidate metadata and, if
// so, widen this erased signature (a localized cold-path change; all callers are in-repo).
using selection_hook = plexus::detail::move_only_function<
    std::size_t(const endpoint &, std::span<const std::size_t>)>;

// The default hook: return the first candidate, so today's single-candidate tiers behave
// identically. It is a small empty callable injected at construction (never a setter).
struct first_candidate
{
    [[nodiscard]] std::size_t operator()(const endpoint &,
                                         std::span<const std::size_t> candidates) const noexcept
    {
        return candidates.front();
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
template <mux_member... Members>
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

    multiplexing_transport(const multiplexing_transport &) = delete;
    multiplexing_transport &operator=(const multiplexing_transport &) = delete;

    void on_accepted(plexus::detail::move_only_function<void(std::unique_ptr<polymorphic_byte_channel>)> cb) { m_on_accepted = std::move(cb); }
    void on_dialed(plexus::detail::move_only_function<void(std::unique_ptr<polymorphic_byte_channel>, const endpoint &)> cb) { m_on_dialed = std::move(cb); }
    void on_dial_failed(plexus::detail::move_only_function<void(const endpoint &, io_error)> cb) { m_on_dial_failed = std::move(cb); }
    void on_error(plexus::detail::move_only_function<void(io_error)> cb) { m_on_error = std::move(cb); }

    void listen(const endpoint &ep) { dispatch(route_of(ep), [&](auto &m) { m.listen(ep); }); }
    void dial(const endpoint &ep)   { dispatch(route_of(ep), [&](auto &m) { m.dial(ep); }); }
    void close() { std::apply([](auto &...m) { (m.close(), ...); }, m_members); }

private:
    template <typename C>
    static std::unique_ptr<polymorphic_byte_channel> wrap(std::unique_ptr<C> ch)
    {
        return std::make_unique<polymorphic_byte_channel>(std::make_unique<channel_adapter<C>>(std::move(ch)));
    }

    template <typename M>
    void wire_member(M &m)
    {
        using C = typename M::channel_type;
        m.on_accepted([this](std::unique_ptr<C> ch) { if(m_on_accepted) m_on_accepted(wrap(std::move(ch))); });
        m.on_dialed([this](std::unique_ptr<C> ch, const endpoint &ep) { if(m_on_dialed) m_on_dialed(wrap(std::move(ch)), ep); });
        m.on_dial_failed([this](const endpoint &ep, io_error e) { if(m_on_dial_failed) m_on_dial_failed(ep, e); });
        m.on_error([this](io_error e) { if(m_on_error) m_on_error(e); });
    }

    // The resolved member tier for an endpoint: locality wins first (same-host -> the
    // local member), so a remote member — including the encrypted ones — never serves a
    // same-host-confined peer even though it rides the remote tier.
    [[nodiscard]] transport_kind tier_of(const endpoint &ep) const noexcept
    {
        return m_selector.select(ep, reliability_hint::unspecified);
    }

    // Search the pack for the members serving ep.scheme within its tier, then let the
    // selection hook pick one. A scheme no member advertises in its tier resolves to no
    // candidate; the dispatch is then a no-op (the caller cannot form that path for a
    // composition that omits the member).
    [[nodiscard]] std::size_t route_of(const endpoint &ep) noexcept
    {
        const transport_kind tier = tier_of(ep);
        std::array<std::size_t, sizeof...(Members)> candidates{};
        std::size_t count = 0;
        collect_candidates(ep, tier, candidates, count, std::index_sequence_for<Members...>{});
        if(count == 0)
            return sizeof...(Members);
        return m_hook(ep, {candidates.data(), count});
    }

    template <std::size_t... I>
    void collect_candidates(const endpoint &ep, transport_kind tier,
                            std::array<std::size_t, sizeof...(Members)> &out, std::size_t &count,
                            std::index_sequence<I...>) const noexcept
    {
        (consider<I>(ep, tier, out, count), ...);
    }

    template <std::size_t I>
    void consider(const endpoint &ep, transport_kind tier,
                  std::array<std::size_t, sizeof...(Members)> &out, std::size_t &count) const noexcept
    {
        using M = std::remove_reference_t<std::tuple_element_t<I, std::tuple<Members...>>>;
        if(M::mux_tier != tier)
            return;
        for(std::string_view scheme : M::mux_schemes)
            if(scheme == ep.scheme)
            {
                out[count++] = I;
                return;
            }
    }

    template <typename F>
    void dispatch(std::size_t index, F &&fn)
    {
        dispatch_at(index, std::forward<F>(fn), std::index_sequence_for<Members...>{});
    }

    template <typename F, std::size_t... I>
    void dispatch_at(std::size_t index, F &&fn, std::index_sequence<I...>)
    {
        ((index == I ? (void)fn(std::get<I>(m_members)) : (void)0), ...);
    }

    std::tuple<Members &...> m_members;
    transport_selector m_selector;
    selection_hook m_hook;
    plexus::detail::move_only_function<void(std::unique_ptr<polymorphic_byte_channel>)> m_on_accepted;
    plexus::detail::move_only_function<void(std::unique_ptr<polymorphic_byte_channel>, const endpoint &)> m_on_dialed;
    plexus::detail::move_only_function<void(const endpoint &, io_error)> m_on_dial_failed;
    plexus::detail::move_only_function<void(io_error)> m_on_error;
};

}

#endif
