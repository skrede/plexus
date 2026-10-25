#ifndef HPP_GUARD_PLEXUS_WITH_IDENTITY_H
#define HPP_GUARD_PLEXUS_WITH_IDENTITY_H

#include "plexus/expected.h"
#include "plexus/wire_bytes.h"
#include "plexus/typed_codec.h"

#include <span>
#include <cstddef>
#include <utility>
#include <system_error>

namespace plexus {

// Gives a codec without a type_info() the type identity a typed endpoint requires.
template<typename Codec>
class with_identity
{
public:
    using value_type = typename Codec::value_type;

    with_identity(Codec codec, type_identity identity)
            : m_codec(std::move(codec))
            , m_identity(identity)
    {
    }

    wire_bytes<> encode(const value_type &value)
    {
        return m_codec.encode(value);
    }

    expected<void, std::error_code> decode(std::span<const std::byte> bytes, value_type &slot)
    {
        return m_codec.decode(bytes, slot);
    }

    type_identity type_info() const
    {
        return m_identity;
    }

private:
    Codec m_codec;
    type_identity m_identity;
};

}

#endif
