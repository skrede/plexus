#ifndef HPP_GUARD_PLEXUS_WIRE_BYTES_H
#define HPP_GUARD_PLEXUS_WIRE_BYTES_H

#include "plexus/wire/frame_reassembler.h"

#include <span>
#include <memory>
#include <cstddef>
#include <utility>

namespace plexus {

// The carrier the receive seam hands up: a non-owning view over wire bytes plus
// a Policy-selected owner handle whose lifetime bounds the view (FORK-M). The
// owner defaults to std::shared_ptr<const void> — the type-erased handle the
// wire reassembler's shared_bytes already exposes — but a Policy may select a
// cheaper owner (intrusive refcount, arena ticket) for the MCU profile. No
// parsing happens here; the bytes are opaque.
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

    // A complete_frame hands its payload up as wire_bytes: the view aliases the
    // shared buffer's bytes, the owner keeps them alive past the source's scope.
    wire_bytes(const wire::shared_bytes &bytes)
            : m_view(static_cast<std::span<const std::byte>>(bytes))
            , m_owner(bytes.owner())
    {
    }

    const std::byte *data() const noexcept { return m_view.data(); }
    std::size_t      size() const noexcept { return m_view.size(); }
    bool             empty() const noexcept { return m_view.empty(); }

    operator std::span<const std::byte>() const noexcept { return m_view; }

    const owner_type &owner() const noexcept { return m_owner; }

private:
    std::span<const std::byte> m_view;
    owner_type                 m_owner{};
};

}

#endif
