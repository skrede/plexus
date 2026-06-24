#ifndef HPP_GUARD_PLEXUS_IO_EPOCH_SOURCE_H
#define HPP_GUARD_PLEXUS_IO_EPOCH_SOURCE_H

#include <cstdint>
#include <type_traits>

namespace plexus::io {

class epoch_source
{
public:
    // Wraps u64-max -> 1, never yielding 0: a zero session_id is reserved for
    // handshake control frames.
    std::uint64_t next() noexcept
    {
        ++m_counter;
        if(m_counter == 0)
            m_counter = 1;
        return m_counter;
    }

    std::uint64_t current() const noexcept
    {
        return m_counter;
    }

private:
    std::uint64_t m_counter{0};
};

static_assert(std::is_same_v<decltype(epoch_source{}.next()), std::uint64_t>, "epoch mint width must stay u64 so the session_id wire field never narrows");

}

#endif
