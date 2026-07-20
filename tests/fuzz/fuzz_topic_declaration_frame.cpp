// libFuzzer harness for the topic-declaration decoder. The verb carries a
// length-prefixed opaque type name and a closed three-state byte over an untrusted
// transport, so a short, oversize, or mis-shaped frame must reject (return nullopt)
// rather than over-read or materialize an unbounded string.

#include "fuzz_sink.h"
#include "plexus/wire/topic_declaration.h"

#include <span>
#include <cstddef>
#include <cstdint>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *Data, size_t Size)
{
    using namespace plexus::wire;

    auto bytes = std::span<const std::byte>{reinterpret_cast<const std::byte *>(Data), Size};

    auto td = decode_topic_declaration(bytes);
    fuzz_consume(td);

    return 0;
}
