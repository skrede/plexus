#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_WARN_LINE_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_WARN_LINE_H

#include "plexus/log/logger.h"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <charconv>
#include <optional>
#include <algorithm>
#include <string_view>
#include <system_error>

namespace plexus::io::detail {

inline constexpr std::chrono::seconds k_malformed_warning_interval{1};

class warn_line
{
public:
    static constexpr std::size_t k_capacity = 128;

    warn_line() noexcept
            : m_size(0)
            , m_buffer()
    {
    }

    void append(std::string_view text) noexcept
    {
        const std::size_t n = std::min(text.size(), k_capacity - m_size);
        std::copy_n(text.data(), n, m_buffer.data() + m_size);
        m_size += n;
    }

    void append_count(std::uint64_t count) noexcept
    {
        const std::to_chars_result result = std::to_chars(m_buffer.data() + m_size, m_buffer.data() + k_capacity, count);
        if(result.ec == std::errc{})
            m_size = static_cast<std::size_t>(result.ptr - m_buffer.data());
    }

    std::string_view view() const noexcept
    {
        return {m_buffer.data(), m_size};
    }

private:
    std::size_t m_size;
    std::array<char, k_capacity> m_buffer;
};

class rate_limited_warning
{
public:
    explicit rate_limited_warning(std::chrono::steady_clock::duration interval) noexcept
            : m_suppressed(0)
            , m_interval(interval)
            , m_last_line()
    {
    }

    void note(log::logger &logger, std::string_view line, std::chrono::steady_clock::time_point now)
    {
        if(m_last_line && now - *m_last_line < m_interval)
        {
            ++m_suppressed;
            return;
        }
        warn_line out;
        out.append(line);
        if(m_suppressed != 0)
        {
            out.append(", ");
            out.append_count(m_suppressed);
            out.append(" more");
        }
        logger.warn(out.view());
        m_last_line  = now;
        m_suppressed = 0;
    }

private:
    std::uint64_t m_suppressed;
    std::chrono::steady_clock::duration m_interval;
    std::optional<std::chrono::steady_clock::time_point> m_last_line;
};

}

#endif
