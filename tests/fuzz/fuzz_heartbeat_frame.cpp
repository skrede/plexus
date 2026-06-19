// libFuzzer harness for the wire/heartbeat decoder. Drives decode_heartbeat over
// a fuzzer-controlled span; the keepalive payload arrives over an untrusted
// transport, so a buffer shorter than the fixed width must reject (return nullopt)
// rather than over-read.

#include "fuzz_sink.h"
#include "plexus/wire/heartbeat.h"

#include <span>
#include <cstddef>
#include <cstdint>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *Data, size_t Size)
{
    using namespace plexus::wire;

    auto bytes = std::span<const std::byte>{reinterpret_cast<const std::byte *>(Data), Size};

    auto hb = decode_heartbeat(bytes);
    fuzz_consume(hb);

    return 0;
}
