#include "fuzz_sink.h"

#include "plexus/wire/companion.h"

#include <span>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <algorithm>

namespace {

using bytes_view = std::span<const std::byte>;

template<std::size_t N>
void require_round_trip(const std::array<std::byte, N> &encoded, bytes_view accepted)
{
    if(accepted.size() != N || !std::equal(encoded.begin(), encoded.end(), accepted.begin()))
        std::abort();
}

template<typename Decode, typename Encode>
void drive(bytes_view payload, Decode decode, Encode encode)
{
    const auto decoded = decode(payload);
    fuzz_consume(decoded);
    if(decoded)
        require_round_trip(encode(*decoded), payload);
}

}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    using namespace plexus::wire;

    if(size == 0)
        return 0;
    const bytes_view payload{reinterpret_cast<const std::byte *>(data) + 1, size - 1};
    switch(data[0] % 3)
    {
        case 0:
            drive(payload, decode_companion, encode_companion);
            break;
        case 1:
            drive(payload, decode_companion_ready, encode_companion_ready);
            break;
        default:
            drive(payload, decode_companion_switch, encode_companion_switch);
            break;
    }
    return 0;
}
