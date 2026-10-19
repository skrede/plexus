#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_UPGRADE_RETRY_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_UPGRADE_RETRY_H

#include "plexus/io/upgrade_channel.h"
#include "plexus/io/demand_transition.h"

#include <map>
#include <chrono>
#include <string>
#include <vector>
#include <compare>
#include <cstdint>
#include <algorithm>
#include <string_view>

namespace plexus::io::detail {

// Re-attempt k of a transient decline lands floor * (2^k - 1) after it; the floor and the cap are the smallest that
// engage a ring whose slab appears up to one second late.
inline constexpr std::chrono::milliseconds k_retry_floor{1};
inline constexpr std::uint32_t k_retry_factor   = 2;
inline constexpr std::uint32_t k_retry_attempts = 10;

// The declines a later attempt can cure; every other reason is final.
constexpr bool retryable(fallback_reason reason) noexcept
{
    return reason == fallback_reason::transient;
}

struct retry_key
{
    std::string node_name;
    std::string fqn;
    demand_role role;

    std::strong_ordering operator<=>(const retry_key &) const = default;
};

// At most one pending re-attempt per (peer, fqn, role). The owner drives one timer, so a wake charges its whole delay to
// every entry; an entry deferred while a wake is armed may therefore come due early, by at most that delay.
class upgrade_retry
{
    struct pending
    {
        std::uint32_t attempts;
        std::chrono::milliseconds backoff;
        std::chrono::milliseconds remaining;
    };

public:
    upgrade_retry() noexcept
            : m_armed(0)
    {
    }

    bool empty() const noexcept
    {
        return m_entries.empty();
    }

    bool contains(const retry_key &key) const
    {
        return m_entries.contains(key);
    }

    void defer(std::string_view node_name, std::string_view fqn, demand_role role)
    {
        m_entries.try_emplace(retry_key{std::string{node_name}, std::string{fqn}, role}, pending{0, k_retry_floor, k_retry_floor});
    }

    // The delay of the wake to arm next; zero while one is armed or nothing is pending.
    std::chrono::milliseconds arm()
    {
        if(m_armed.count() != 0 || m_entries.empty())
            return std::chrono::milliseconds{0};
        const auto soonest = std::ranges::min_element(m_entries, {}, [](const auto &entry) { return entry.second.remaining; });
        m_armed            = soonest->second.remaining;
        return m_armed;
    }

    // Charges the armed delay and returns the pairs it brought due.
    std::vector<retry_key> take_due()
    {
        std::vector<retry_key> due;
        for(auto &[key, entry] : m_entries)
        {
            entry.remaining = entry.remaining > m_armed ? entry.remaining - m_armed : std::chrono::milliseconds{0};
            if(entry.remaining.count() == 0)
                due.push_back(key);
        }
        m_armed = std::chrono::milliseconds{0};
        return due;
    }

    // An engaged or finally declined pair leaves the table; a transient one backs off until the cap, where it leaves with
    // retries_exhausted. Returns the reason the pair now stands on.
    fallback_reason settle(const retry_key &key, fallback_reason reason)
    {
        auto it = m_entries.find(key);
        if(it == m_entries.end())
            return reason;
        if(!retryable(reason) || ++it->second.attempts >= k_retry_attempts)
        {
            m_entries.erase(it);
            return retryable(reason) ? fallback_reason::retries_exhausted : reason;
        }
        it->second.backoff *= k_retry_factor;
        it->second.remaining = it->second.backoff;
        return reason;
    }

    void erase_peer(std::string_view node_name)
    {
        std::erase_if(m_entries, [&](const auto &entry) { return entry.first.node_name == node_name; });
    }

    void erase_fqn(std::string_view fqn)
    {
        std::erase_if(m_entries, [&](const auto &entry) { return entry.first.fqn == fqn; });
    }

    // The peers whose pending re-attempt for the fqn and role leaves the table, served by a lane another attempt minted.
    std::vector<std::string> take_waiting(std::string_view fqn, demand_role role)
    {
        std::vector<std::string> waiting;
        for(const auto &[key, entry] : m_entries)
            if(key.fqn == fqn && key.role == role)
                waiting.push_back(key.node_name);
        std::erase_if(m_entries, [&](const auto &entry) { return entry.first.fqn == fqn && entry.first.role == role; });
        return waiting;
    }

    void erase_pair(std::string_view node_name, std::string_view fqn, demand_role role)
    {
        std::erase_if(m_entries, [&](const auto &entry) { return entry.first.node_name == node_name && entry.first.fqn == fqn && entry.first.role == role; });
    }

private:
    std::chrono::milliseconds m_armed;
    std::map<retry_key, pending> m_entries;
};

}

#endif
