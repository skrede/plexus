// libFuzzer harness for the LEB128 varint decoder — the bounds gate for the
// flag-gated source-identity endpoint counter on the data frame. It drives
// read_varint over a fuzzer-controlled span with a fuzzer-chosen starting offset,
// and asserts the decoder's bounds contract: on any input it never over-reads, and
// `consumed` is never advanced past the buffer (only on success, by exactly the
// bytes the encoding occupied).

#include "plexus/wire/varint.h"

#include <cstddef>
#include <cstdint>
#include <span>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *Data, size_t Size)
{
    using namespace plexus::wire;

    auto bytes = std::span<const std::byte>{
        reinterpret_cast<const std::byte *>(Data), Size};

    // Derive a starting offset from the first byte so the consumed-offset bounds path
    // (consumed may legitimately equal Size) is exercised alongside the body decode.
    std::size_t consumed = Size == 0 ? 0 : (static_cast<std::size_t>(Data[0]) % (Size + 1));
    const std::size_t before = consumed;

    auto value = read_varint(bytes, consumed);
    (void)value;

    // The decoder must never advance consumed past the buffer, and must not move it on
    // failure (nullopt leaves it where it was so the caller can discriminate underrun).
    if (consumed > Size)
        __builtin_trap();
    if (!value && consumed != before)
        __builtin_trap();
    return 0;
}
