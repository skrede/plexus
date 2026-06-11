#ifndef HPP_GUARD_PLEXUS_SUBSCRIBER_H
#define HPP_GUARD_PLEXUS_SUBSCRIBER_H

#include "plexus/node.h"

#include "plexus/io/message_info.h"
#include "plexus/io/subscriber_qos.h"

#include "plexus/policy.h"

#include "plexus/detail/compat.h"

#include <span>
#include <cstddef>
#include <utility>
#include <type_traits>
#include <string_view>

namespace plexus {

// A move-only RAII subscribing endpoint (D-10/D-11): the CONSTRUCTOR is the
// registration — it installs a STANDING topic-level demand on the node, which fans the
// per-peer subscribe to every known peer now and to each later-discovered peer with no
// further user action (D-01). The callback mirrors the Phase 25 receive seam (D-14): it
// accepts EITHER a 2-arg `void(span<const std::byte>)` or a 3-arg
// `void(span<const std::byte>, const message_info&)` callable, dispatched at
// construction with if constexpr — a 2-arg callback costs nothing for the metadata it
// ignores. Templated on Policy alone, so one subscriber type serves any transport pack.
//
// LIFETIME (D-13): a subscriber must NOT outlive its node. The canonical usage is
// member-init aggregation (node ref first, handles after), so reverse destruction
// retires the demand before the node. Dropping the handle retires the demand: it stops
// the callback and, when it was the last local subscriber for the fqn, unsubscribes the
// topic from every fanned peer. A moved-from handle is inert (empty retire); its
// destructor does nothing, so no callback ever fires through a dropped subscriber.
template <typename Policy>
    requires plexus::Policy<Policy>
class subscriber
{
public:
    template <typename... NodeTs, typename Cb>
    subscriber(node<Policy, NodeTs...> &n, std::string_view fqn, Cb cb)
        : subscriber(n, fqn, io::subscriber_qos{}, std::move(cb))
    {
    }

    template <typename... NodeTs, typename Cb>
    subscriber(node<Policy, NodeTs...> &n, std::string_view fqn, const io::subscriber_qos &qos, Cb cb)
    {
        const auto rid = n.register_subscriber_seam(fqn, qos, adapt(std::move(cb)));
        m_retire = [&n, rid] { n.retire_subscriber_seam(rid); };
    }

    subscriber(subscriber &&) noexcept = default;
    subscriber &operator=(subscriber &&) noexcept = default;

    subscriber(const subscriber &) = delete;
    subscriber &operator=(const subscriber &) = delete;

    ~subscriber()
    {
        if(m_retire != nullptr)
            m_retire();
    }

private:
    // Normalize the user callback to the node's 3-arg demux shape. A 3-arg callable is
    // forwarded the message_info; a 2-arg callable is wrapped to drop it. The arity is
    // resolved ONCE here (the cold registration path), so the hot demux fans a uniform
    // signature with no per-frame branch.
    template <typename Cb>
    static plexus::detail::move_only_function<void(std::span<const std::byte>, const io::message_info &)>
    adapt(Cb cb)
    {
        if constexpr(std::is_invocable_v<Cb &, std::span<const std::byte>, const io::message_info &>)
            return [cb = std::move(cb)](std::span<const std::byte> bytes, const io::message_info &info) mutable
            { cb(bytes, info); };
        else
            return [cb = std::move(cb)](std::span<const std::byte> bytes, const io::message_info &) mutable
            { cb(bytes); };
    }

    plexus::detail::move_only_function<void()> m_retire;
};

}

#endif
