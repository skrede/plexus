// libFuzzer harness for the wire/fetch_latched decoder. Drives
// decode_fetch_latched_request over a fuzzer-controlled span; max_samples is an
// attacker-controlled count over an untrusted transport, so a frame shorter than
// the fixed 12 bytes must reject (return nullopt) rather than over-read.

#include "fuzz_sink.h"
#include "plexus/wire/fetch_latched.h"

#include <span>
#include <cstddef>
#include <cstdint>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *Data, size_t Size)
{
    using namespace plexus::wire;

    auto bytes = std::span<const std::byte>{
        reinterpret_cast<const std::byte *>(Data), Size};

    auto req = decode_fetch_latched_request(bytes);
    fuzz_consume(req);

    return 0;
}
