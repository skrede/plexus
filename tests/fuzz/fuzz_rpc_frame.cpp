// libFuzzer harness for the wire/rpc_frame decoders. Drives both
// decode_rpc_request and decode_rpc_response over the same fuzzer-controlled
// span; a fresh decode per call holds no shared state across inputs.

#include "fuzz_sink.h"
#include "plexus/wire/rpc_frame.h"

#include <cstddef>
#include <cstdint>
#include <span>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *Data, size_t Size)
{
    using namespace plexus::wire;

    auto bytes = std::span<const std::byte>{
        reinterpret_cast<const std::byte *>(Data), Size};

    auto req = decode_rpc_request(bytes);
    fuzz_consume(req);

    auto resp = decode_rpc_response(bytes);
    fuzz_consume(resp);

    return 0;
}
