// libFuzzer harness for the wire/peer_report decoder. Drives decode_peer_report over a
// fuzzer-controlled span; the report arrives over an untrusted authenticated session, so a
// malformed or truncated frame must reject (return nullopt) rather than over-read, over-reserve,
// or yield a partial struct.

#include "fuzz_sink.h"
#include "plexus/wire/peer_report.h"

#include <span>
#include <cstddef>
#include <cstdint>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *Data, size_t Size)
{
    using namespace plexus::wire;

    auto bytes = std::span<const std::byte>{reinterpret_cast<const std::byte *>(Data), Size};

    auto report = decode_peer_report(bytes);
    fuzz_consume(report);

    return 0;
}
