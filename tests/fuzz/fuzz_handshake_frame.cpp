// libFuzzer harness for the wire/handshake decoders. Drives both
// decode_handshake_request and decode_handshake_response over the same
// fuzzer-controlled span; a fresh decode per call holds no shared state across inputs.

#include "plexus/wire/handshake.h"

#include <span>
#include <cstddef>
#include <cstdint>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *Data, size_t Size)
{
    using namespace plexus::wire;

    auto bytes = std::span<const std::byte>{
        reinterpret_cast<const std::byte *>(Data), Size};

    auto req = decode_handshake_request(bytes);
    (void)req;

    auto resp = decode_handshake_response(bytes);
    (void)resp;

    return 0;
}
