// libFuzzer harness for the wire/handshake decoders. Drives both
// decode_handshake_request and decode_handshake_response over the same
// fuzzer-controlled span; a fresh decode per call holds no shared state across inputs.
// The frame is the WIDENED layout (the appended attach region: key_id + own_nonce +
// cipher_offer + chosen_cipher after the same-host fingerprint): the decoders
// bounds-check against the widened request_size/response_size, so a short or oversize
// frame must reject (return nullopt), never OOB-read. The fuzzer explores the input
// space around the new boundary; this harness needs no shape change for the wider
// region because it decodes raw bytes, but the wider decode is exactly what it covers.

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
