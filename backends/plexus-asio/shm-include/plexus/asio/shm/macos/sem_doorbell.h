#ifndef HPP_GUARD_PLEXUS_ASIO_SHM_MACOS_SEM_DOORBELL_H
#define HPP_GUARD_PLEXUS_ASIO_SHM_MACOS_SEM_DOORBELL_H

#include "plexus/shm/ring_layout.h"
#include "plexus/shm/doorbell_naming.h"

#include <fcntl.h>
#include <semaphore.h>

#include <array>
#include <mutex>
#include <cerrno>
#include <random>
#include <cstdint>
#include <optional>
#include <exception>
#include <string_view>

namespace plexus::asio::shm::detail {

// The waiter's own named semaphore. It is only ever created exclusively, never opened by name, so a stale or squatted
// name costs a fresh tag instead of an adopted count.
class sem_doorbell
{
public:
    sem_doorbell() noexcept
            : m_sem(SEM_FAILED)
            , m_name()
    {
    }

    ~sem_doorbell()
    {
        close();
    }

    sem_doorbell(const sem_doorbell &)            = delete;
    sem_doorbell &operator=(const sem_doorbell &) = delete;

    template<typename TagSource>
    std::optional<std::uint32_t> create(std::string_view region, std::uint32_t slot, TagSource &&next_tag)
    {
        for(std::uint32_t attempt = 0; attempt < ::plexus::shm::k_doorbell_create_attempts; ++attempt)
        {
            const std::uint32_t tag = next_tag();
            m_name                  = ::plexus::shm::format_doorbell_name(::plexus::shm::k_semaphore_doorbell_prefix, region, slot, tag);
            m_sem                   = ::sem_open(m_name.c_str(), O_CREAT | O_EXCL, 0600, 0);
            if(m_sem != SEM_FAILED)
                return tag;
            if(errno != EEXIST)
                return std::nullopt;
        }
        return std::nullopt;
    }

    // An entropy source that cannot start leaves the waiter without a doorbell, as an exhausted resource would.
    std::optional<std::uint32_t> create(std::string_view region, std::uint32_t slot) noexcept
    {
        try
        {
            std::random_device entropy;
            return create(region, slot, [&entropy] { return static_cast<std::uint32_t>(entropy()); });
        }
        catch(const std::exception &)
        {
            return std::nullopt;
        }
    }

    bool created() const noexcept
    {
        return m_sem != SEM_FAILED;
    }

    const ::plexus::shm::doorbell_name &name() const noexcept
    {
        return m_name;
    }

    void post() noexcept
    {
        ::sem_post(m_sem);
    }

    void wait() noexcept
    {
        ::sem_wait(m_sem);
    }

    void close() noexcept
    {
        if(m_sem == SEM_FAILED)
            return;
        ::sem_close(m_sem);
        ::sem_unlink(m_name.c_str());
        m_sem = SEM_FAILED;
    }

private:
    sem_t *m_sem;
    ::plexus::shm::doorbell_name m_name;
};

// The producer's handle per cursor slot, opened by name only when the slot's waiter tag changes, so a repeat wake
// formats and opens nothing. It never creates a name, and a name it cannot open skips that slot.
class sem_doorbell_cache
{
    struct entry
    {
        entry() noexcept
                : tag(0)
                , sem(SEM_FAILED)
        {
        }

        std::uint32_t tag;
        sem_t *sem;
    };

public:
    sem_doorbell_cache() noexcept
            : m_mutex()
            , m_entries()
    {
    }

    ~sem_doorbell_cache()
    {
        for(entry &e : m_entries)
            close(e);
    }

    sem_doorbell_cache(const sem_doorbell_cache &)            = delete;
    sem_doorbell_cache &operator=(const sem_doorbell_cache &) = delete;

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
        if(e.sem == SEM_FAILED || e.tag != tag)
        {
            close(e);
            e.tag = tag;
            e.sem = ::sem_open(::plexus::shm::format_doorbell_name(::plexus::shm::k_semaphore_doorbell_prefix, region, slot, tag).c_str(), 0);
        }
        if(e.sem != SEM_FAILED)
            ::sem_post(e.sem);
    }

    static void close(entry &e) noexcept
    {
        if(e.sem != SEM_FAILED)
            ::sem_close(e.sem);
        e.sem = SEM_FAILED;
    }
};

}

#endif
