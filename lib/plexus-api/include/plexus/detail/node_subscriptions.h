#ifndef HPP_GUARD_PLEXUS_DETAIL_NODE_SUBSCRIPTIONS_H
#define HPP_GUARD_PLEXUS_DETAIL_NODE_SUBSCRIPTIONS_H

#include "plexus/detail/node_internals.h"

#include "plexus/io/subscriber_qos.h"

#include <span>
#include <string>
#include <vector>
#include <cstdint>
#include <utility>
#include <optional>
#include <algorithm>
#include <string_view>

namespace plexus::detail {

// While a dispatch fan is live (m_dispatch_depth > 0) the (un)subscribe seams cannot touch
// m_subscriptions directly: a push_back could reallocate it while the callback loop iterates it,
// and an erase would relocate the very callable being invoked. Edits are staged and applied by the
// outermost fan on exit; a logically-removed entry is skipped for the remainder of the fan.
class subscription_table
{
public:
    using registration_id = std::uint64_t;
    using entry           = std::pair<registration_id, subscription>;

    struct dispatch_guard
    {
        subscription_table &owner;
        explicit dispatch_guard(subscription_table &table) noexcept
                : owner(table)
        {
            ++owner.m_dispatch_depth;
        }
        dispatch_guard(const dispatch_guard &)            = delete;
        dispatch_guard &operator=(const dispatch_guard &) = delete;
        ~dispatch_guard()
        {
            if(--owner.m_dispatch_depth != 0)
                return;
#if defined(__cpp_exceptions)
            try
            {
                owner.apply_deferred_edits();
            }
            catch(...)
            {
            }
#else
            owner.apply_deferred_edits();
#endif
        }
    };

    subscription_table()
            : m_dispatch_depth(0)
            , m_next_registration(1)
    {
    }

    registration_id add(subscription sub)
    {
        const registration_id rid = m_next_registration++;
        entry e{rid, std::move(sub)};
        if(m_dispatch_depth > 0)
            m_deferred_adds.push_back(std::move(e));
        else
            m_subscriptions.push_back(std::move(e));
        return rid;
    }

    // Returns the fqn when this removal left it with no live subscriber, so its demand retires.
    std::optional<std::string> remove(registration_id rid)
    {
        if(m_dispatch_depth > 0)
        {
            // A retire from inside a dispatch fan: record a logical removal and defer the physical
            // erase, so the in-flight callback loop never relocates m_subscriptions under itself.
            std::optional<std::string> fqn = fqn_of(rid);
            if(!fqn || pending_removed(rid))
                return std::nullopt;
            m_deferred_removes.push_back(rid);
            if(any_for(*fqn))
                return std::nullopt;
            return fqn;
        }

        auto it = std::find_if(m_subscriptions.begin(), m_subscriptions.end(), [&](const auto &e) { return e.first == rid; });
        if(it == m_subscriptions.end())
            return std::nullopt;
        std::string fqn = it->second.fqn;
        m_subscriptions.erase(it);
        if(any_for(fqn))
            return std::nullopt;
        return fqn;
    }

    bool pending_removed(registration_id rid) const
    {
        return std::find(m_deferred_removes.begin(), m_deferred_removes.end(), rid) != m_deferred_removes.end();
    }

    bool any_for(std::string_view fqn) const
    {
        const auto live = [&](const auto &e) { return e.second.fqn == fqn && !pending_removed(e.first); };
        return std::any_of(m_subscriptions.begin(), m_subscriptions.end(), live) || std::any_of(m_deferred_adds.begin(), m_deferred_adds.end(), live);
    }

    io::subscriber_qos qos_for(std::string_view fqn) const
    {
        for(const auto &[rid, sub] : m_subscriptions)
            if(sub.fqn == fqn)
                return sub.qos;
        return {};
    }

    std::optional<std::uint64_t> type_id_for(std::string_view fqn) const
    {
        for(const auto &[rid, sub] : m_subscriptions)
            if(sub.fqn == fqn)
                return sub.type_id;
        return std::nullopt;
    }

    // Includes logically-removed entries; a dispatch fan skips those through pending_removed.
    std::span<entry> entries() noexcept
    {
        return m_subscriptions;
    }

private:
    std::uint32_t m_dispatch_depth;
    std::vector<entry> m_subscriptions;
    std::vector<entry> m_deferred_adds;
    registration_id m_next_registration;
    std::vector<registration_id> m_deferred_removes;

    std::optional<std::string> fqn_of(registration_id rid) const
    {
        for(const auto &e : m_subscriptions)
            if(e.first == rid)
                return e.second.fqn;
        for(const auto &e : m_deferred_adds)
            if(e.first == rid)
                return e.second.fqn;
        return std::nullopt;
    }

    void apply_deferred_edits()
    {
        if(!m_deferred_removes.empty())
        {
            std::erase_if(m_subscriptions, [&](const auto &e) { return pending_removed(e.first); });
            std::erase_if(m_deferred_adds, [&](const auto &e) { return pending_removed(e.first); });
            m_deferred_removes.clear();
        }
        for(auto &e : m_deferred_adds)
            m_subscriptions.push_back(std::move(e));
        m_deferred_adds.clear();
    }
};

}

#endif
