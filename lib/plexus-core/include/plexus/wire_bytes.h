#ifndef HPP_GUARD_PLEXUS_WIRE_BYTES_H
#define HPP_GUARD_PLEXUS_WIRE_BYTES_H

#include "plexus/wire/frame_reassembler.h"

#include <span>
#include <memory>
#include <cstddef>
#include <utility>

namespace plexus {

// A non-owning view over wire bytes plus an owner handle whose lifetime bounds
// the view.
template<typename ByteOwner = std::shared_ptr<const void>>
class wire_bytes
{
public:
    using owner_type = ByteOwner;

    wire_bytes() = default;

    wire_bytes(std::span<const std::byte> view, owner_type owner)
            : m_view(view)
            , m_owner(std::move(owner))
    {
    }

    wire_bytes(const wire::shared_bytes &bytes)
            : m_view(static_cast<std::span<const std::byte>>(bytes))
            , m_owner(bytes.owner())
    {
    }

    const std::byte *data() const noexcept
    {
        return m_view.data();
    }
    std::size_t size() const noexcept
    {
        return m_view.size();
    }
    bool empty() const noexcept
    {
        return m_view.empty();
    }

    operator std::span<const std::byte>() const noexcept
    {
        return m_view;
    }

    const owner_type &owner() const noexcept
    {
        return m_owner;
    }

private:
    std::span<const std::byte> m_view;
    owner_type m_owner{};
};

}

#endif
