#ifndef HPP_GUARD_PLEXUS_ASIO_SHM_WINDOWS_EVENT_DOORBELL_H
#define HPP_GUARD_PLEXUS_ASIO_SHM_WINDOWS_EVENT_DOORBELL_H

#include "plexus/native/detail/win_owner_only_security.h"

#include "plexus/shm/ring_layout.h"
#include "plexus/shm/doorbell_naming.h"

#include <windows.h>

#include <array>
#include <mutex>
#include <random>
#include <cstdint>
#include <optional>
#include <algorithm>
#include <exception>
#include <string_view>

namespace plexus::asio::shm::detail {

inline constexpr std::wstring_view k_event_doorbell_namespace = L"Local\\pxw.";

using event_doorbell_name = std::array<wchar_t, 40>;

static_assert(k_event_doorbell_namespace.size() + ::plexus::shm::k_doorbell_key_chars < event_doorbell_name{}.size(), "the event name must keep its terminator");

inline event_doorbell_name event_doorbell_name_for(std::string_view region, std::uint32_t slot, std::uint32_t tag) noexcept
{
    const ::plexus::shm::doorbell_name key = ::plexus::shm::format_doorbell_name({}, region, slot, tag);
    event_doorbell_name name{};
    std::copy(k_event_doorbell_namespace.begin(), k_event_doorbell_namespace.end(), name.begin());
    std::copy_n(key.chars.begin(), key.size, name.begin() + k_event_doorbell_namespace.size());
    return name;
}

struct own_event
{
    HANDLE handle;
    std::uint32_t tag;
};

// Any object already holding the name, an event or not, is a collision the waiter steps past with a fresh tag.
inline bool event_name_taken(HANDLE event) noexcept
{
    const DWORD error = ::GetLastError();
    return error == ERROR_ALREADY_EXISTS || (event == nullptr && (error == ERROR_ACCESS_DENIED || error == ERROR_INVALID_HANDLE));
}

// The waiter's own auto-reset event: CreateEventW with bManualReset FALSE resets the event when a wait is satisfied.
// It is owner-only and created exclusively, so a name another object already holds is never adopted.
template<typename TagSource>
std::optional<own_event> create_own_event(std::string_view region, std::uint32_t slot, TagSource &&next_tag)
{
    ::plexus::native::detail::win_owner_only_security security;
    for(std::uint32_t attempt = 0; security.get() != nullptr && attempt < ::plexus::shm::k_doorbell_create_attempts; ++attempt)
    {
        const std::uint32_t tag = next_tag();
        HANDLE event            = ::CreateEventW(security.get(), FALSE, FALSE, event_doorbell_name_for(region, slot, tag).data());
        const bool taken        = event_name_taken(event);
        if(event != nullptr && !taken)
            return own_event{event, tag};
        if(event != nullptr)
            ::CloseHandle(event);
        if(!taken)
            return std::nullopt;
    }
    return std::nullopt;
}

// An entropy source that cannot start leaves the waiter without a doorbell, as an exhausted resource would.
inline std::optional<own_event> create_own_event(std::string_view region, std::uint32_t slot) noexcept
{
    try
    {
        std::random_device entropy;
        return create_own_event(region, slot, [&entropy] { return static_cast<std::uint32_t>(entropy()); });
    }
    catch(const std::exception &)
    {
        return std::nullopt;
    }
}

// The producer's handle per cursor slot, opened by name only when the slot's waiter tag changes, so a repeat wake
// formats and opens nothing. It never creates a name, and a name it cannot open skips that slot.
class event_doorbell_cache
{
    struct entry
    {
        entry() noexcept
                : tag(0)
                , event(nullptr)
        {
        }

        std::uint32_t tag;
        HANDLE event;
    };

public:
    event_doorbell_cache() noexcept
            : m_mutex()
            , m_entries()
    {
    }

    ~event_doorbell_cache()
    {
        for(entry &e : m_entries)
            close(e);
    }

    event_doorbell_cache(const event_doorbell_cache &)            = delete;
    event_doorbell_cache &operator=(const event_doorbell_cache &) = delete;

    // The mask comes from the validated parked-slot scan; each tag is read after its slot was seen PARKED.
    void ring(const ::plexus::shm::control_header_t &header, std::string_view region, std::uint32_t mask) noexcept
    {
        const std::lock_guard<std::mutex> lock(m_mutex);
        for(std::uint32_t slot = 0; mask != 0 && slot < m_entries.size(); ++slot, mask >>= 1)
        {
            if((mask & 1u) != 0)
                ring_slot(region, slot, header.cursors[slot].waiter_tag.load(std::memory_order_acquire));
        }
    }

private:
    std::mutex m_mutex;
    std::array<entry, ::plexus::shm::k_max_consumers> m_entries;

    void ring_slot(std::string_view region, std::uint32_t slot, std::uint32_t tag) noexcept
    {
        entry &e = m_entries[slot];
        if(e.event == nullptr || e.tag != tag)
        {
            close(e);
            e.tag   = tag;
            e.event = ::OpenEventW(EVENT_MODIFY_STATE, FALSE, event_doorbell_name_for(region, slot, tag).data());
        }
        if(e.event != nullptr)
            ::SetEvent(e.event);
    }

    static void close(entry &e) noexcept
    {
        if(e.event != nullptr)
            ::CloseHandle(e.event);
        e.event = nullptr;
    }
};

}

#endif
