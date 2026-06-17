#ifndef HPP_GUARD_TESTS_INTEGRATION_IN_MEMORY_BYTE_SINK_H
#define HPP_GUARD_TESTS_INTEGRATION_IN_MEMORY_BYTE_SINK_H

#include "plexus/io/recording/byte_sink.h"

#include <span>
#include <vector>
#include <cstddef>

// The non-disk drain target the round-trip / drain tests assert against: it
// appends every write into one growing buffer, proving an MCU serial/SD binding
// is a drop-in (the seam is a raw byte_sink, nothing disk-specific). It exposes
// the accumulated bytes for inspection; it never adds a test-only mutator.
class in_memory_byte_sink final : public plexus::io::recording::byte_sink
{
public:
    void write(std::span<const std::byte> bytes) override
    {
        m_bytes.insert(m_bytes.end(), bytes.begin(), bytes.end());
    }

    [[nodiscard]] std::span<const std::byte> bytes() const noexcept { return m_bytes; }

private:
    std::vector<std::byte> m_bytes;
};

#endif
