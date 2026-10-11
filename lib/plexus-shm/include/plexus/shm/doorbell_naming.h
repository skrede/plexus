#ifndef HPP_GUARD_PLEXUS_SHM_DOORBELL_NAMING_H
#define HPP_GUARD_PLEXUS_SHM_DOORBELL_NAMING_H

#include "plexus/shm/ring_layout.h"

#include <array>
#include <cstddef>
#include <cstdint>
#include <algorithm>
#include <string_view>

namespace plexus::shm {

// The macOS semaphore name budget, leading slash included (xnu bsd/kern/posix_sem.c, PSEMNAMLEN).
inline constexpr std::size_t k_doorbell_name_max              = 30;
inline constexpr std::size_t k_doorbell_region_chars          = 16;
inline constexpr std::size_t k_doorbell_key_chars             = k_doorbell_region_chars + 1 + 8;
inline constexpr std::uint32_t k_doorbell_create_attempts     = 8;
inline constexpr std::string_view k_semaphore_doorbell_prefix = "/pxw";

static_assert(k_max_consumers <= 16, "a doorbell name spends one hex digit on the cursor slot");
static_assert(k_semaphore_doorbell_prefix.size() + k_doorbell_key_chars <= k_doorbell_name_max, "the semaphore doorbell name must fit the macOS budget");

struct doorbell_name
{
    std::array<char, 32> chars;
    std::size_t size;

    const char *c_str() const noexcept
    {
        return chars.data();
    }

    std::string_view view() const noexcept
    {
        return {chars.data(), size};
    }
};

namespace detail {

inline void append_text(doorbell_name &name, std::string_view text, std::size_t limit) noexcept
{
    const std::size_t count = text.size() < limit ? text.size() : limit;
    std::copy_n(text.data(), count, name.chars.data() + name.size);
    name.size += count;
}

inline void append_hex(doorbell_name &name, std::uint32_t value, std::uint32_t digits) noexcept
{
    constexpr std::string_view k_digits = "0123456789abcdef";
    for(std::uint32_t digit = digits; digit-- > 0;)
        name.chars[name.size++] = k_digits[(value >> (digit * 4)) & 0xfu];
}

}

// A waiter's doorbell is keyed by its region, its cursor slot and the tag it stored in that cursor, so every
// registration names a fresh object; the region contributes its first 16 characters.
inline doorbell_name format_doorbell_name(std::string_view prefix, std::string_view region, std::uint32_t slot, std::uint32_t tag) noexcept
{
    doorbell_name name{};
    detail::append_text(name, prefix, k_doorbell_name_max - k_doorbell_key_chars);
    detail::append_text(name, region, k_doorbell_region_chars);
    detail::append_hex(name, slot, 1);
    detail::append_hex(name, tag, 8);
    return name;
}

}

#endif
