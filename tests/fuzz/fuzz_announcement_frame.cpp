// libFuzzer harness for the wire/announcement decoder. Drives decode_announcement over
// a fuzzer-controlled span; the announcement arrives over an untrusted multicast group,
// so a malformed or truncated datagram must reject (return nullopt) rather than over-read
// or yield a partial struct.

#include "fuzz_sink.h"
#include "plexus/wire/announcement.h"

#include <span>
#include <cstddef>
#include <cstdint>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *Data, size_t Size)
{
    using namespace plexus::wire;

    auto bytes = std::span<const std::byte>{reinterpret_cast<const std::byte *>(Data), Size};

    auto ann = decode_announcement(bytes);
    fuzz_consume(ann);

    return 0;
}
