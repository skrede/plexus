// libFuzzer harness for the wire/data_frame decoders. Dispatches between
// decode_unidirectional and decode_bidirectional on the low bit of the
// first input byte.

#include "plexus/wire/data_frame.h"

#include <cstddef>
#include <cstdint>
#include <span>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *Data, size_t Size)
{
    using namespace plexus::wire;

    auto bytes = std::span<const std::byte>{
        reinterpret_cast<const std::byte *>(Data), Size};

    if (Size == 0 || (Data[0] & 0x01) == 0)
    {
        auto result = decode_unidirectional(bytes);
        (void)result;
    }
    else
    {
        auto result = decode_bidirectional(bytes.subspan(1));
        (void)result;
    }
    return 0;
}
