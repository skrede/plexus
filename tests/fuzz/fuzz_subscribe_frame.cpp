// libFuzzer harness for the wire/subscribe decoders. Drives both
// decode_subscribe_request (the flag-gated trailing QoS region) and
// decode_subscribe_response (the optional trailing degraded byte) over the same
// fuzzer-controlled span; both carry length-prefixed and optional-trailing fields
// over an untrusted transport, so a short, oversize, or mis-shaped frame must
// reject (return nullopt) rather than over-read.

#include "plexus/wire/subscribe.h"

#include <span>
#include <cstddef>
#include <cstdint>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *Data, size_t Size)
{
    using namespace plexus::wire;

    auto bytes = std::span<const std::byte>{
        reinterpret_cast<const std::byte *>(Data), Size};

    auto req = decode_subscribe_request(bytes);
    (void)req;

    auto resp = decode_subscribe_response(bytes);
    (void)resp;

    return 0;
}
