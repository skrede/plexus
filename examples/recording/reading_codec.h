#ifndef HPP_GUARD_EXAMPLES_RECORDING_READING_CODEC_H
#define HPP_GUARD_EXAMPLES_RECORDING_READING_CODEC_H

#include "plexus/expected.h"
#include "plexus/wire_bytes.h"
#include "plexus/typed_codec.h"

#include <span>
#include <memory>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <system_error>

struct reading
{
    std::uint32_t sensor{};
    std::uint32_t value{};
};

struct reading_codec
{
    using value_type = reading;

    plexus::wire_bytes<> encode(const reading &r) const
    {
        auto owner = std::make_shared<std::vector<std::byte>>(8);
        for(int i = 0; i < 4; ++i)
        {
            (*owner)[i]     = static_cast<std::byte>((r.sensor >> (8 * i)) & 0xff);
            (*owner)[4 + i] = static_cast<std::byte>((r.value >> (8 * i)) & 0xff);
        }
        const std::span<const std::byte> view{owner->data(), owner->size()};
        return {view, std::move(owner)};
    }

    plexus::expected<void, std::error_code> decode(std::span<const std::byte> b, reading &out) const
    {
        if(b.size() != 8)
            return plexus::expected<void, std::error_code>{
                plexus::unexpect, std::make_error_code(std::errc::invalid_argument)};
        auto u32 = [&](int o)
        {
            std::uint32_t v = 0;
            for(int i = 0; i < 4; ++i)
                v |= static_cast<std::uint32_t>(std::to_integer<std::uint8_t>(b[o + i])) << (8 * i);
            return v;
        };
        out = {u32(0), u32(4)};
        return {};
    }

    plexus::type_identity type_info() const { return {0x5E4501u, "reading"}; }
};

#endif
