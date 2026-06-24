// libFuzzer harness for frame_reassembler::feed. The reassembler is
// constructed FRESH per call to keep libFuzzer's per-input crash isolation
// clean -- a static instance would persist UB across inputs and confuse
// libFuzzer's per-input crash isolation.

#include "plexus/wire/frame_reassembler.h"

#include <cstddef>
#include <cstdint>
#include <span>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *Data, size_t Size)
{
    using namespace plexus::wire;

    auto bytes = std::span<const std::byte>{reinterpret_cast<const std::byte *>(Data), Size};

    frame_reassembler r;
    auto result = r.feed(bytes);
    (void)result;
    return 0;
}
