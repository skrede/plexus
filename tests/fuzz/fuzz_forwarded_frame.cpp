// libFuzzer harness for the wire/forwarded_frame decoder. Drives decode_forwarded_frame over a
// fuzzer-controlled span; the frame arrives over an untrusted authenticated session, so a malformed
// or truncated frame must reject (return nullopt) rather than over-read, over-reserve, or yield a
// partial struct.

#include "fuzz_sink.h"
#include "plexus/wire/forwarded_frame.h"

#include <span>
#include <cstddef>
#include <cstdint>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *Data, size_t Size)
{
    using namespace plexus::wire;

    auto bytes = std::span<const std::byte>{reinterpret_cast<const std::byte *>(Data), Size};

    auto forwarded = decode_forwarded_frame(bytes);
    fuzz_consume(forwarded);

    return 0;
}
