#ifndef HPP_GUARD_PLEXUS_EXAMPLES_MCU_ONBOARD_BENCH_WORKLOAD_H
#define HPP_GUARD_PLEXUS_EXAMPLES_MCU_ONBOARD_BENCH_WORKLOAD_H

#include "plexus/wire_bytes.h"
#include "plexus/expected.h"
#include "plexus/typed_codec.h"

#include "esp_timer.h"

#include <span>
#include <array>
#include <atomic>
#include <memory>
#include <cstdio>
#include <cstddef>
#include <cstdint>
#include <system_error>

namespace example {

// The fixed payload tiers, mirroring the lwIP/serial round-trip bench so the onboard self-route
// numbers sit in the same table. The largest tier sizes the in-place message buffer.
inline constexpr std::array<std::size_t, 3> k_payload_tiers{16, 256, 4096};
inline constexpr std::size_t k_max_tier_bytes = 4096;

// A warm-up the aggregation discards, then the sampled round trips. One request in flight at a time
// keeps the measurement a clean request->reply self-route round trip.
inline constexpr std::uint32_t k_warmup_requests  = 8;
inline constexpr std::uint32_t k_sampled_requests = 128;

// The onboard message: an in-place tier-sized buffer with the active length and a sequence tag. On
// the intra-node typed lane the subscriber receives THIS object by address — no buffer copy, no
// codec encode — so the buffer rides the self-route as a borrowed object, not a wire frame.
struct message
{
    std::array<std::byte, k_max_tier_bytes> buffer{};
    std::uint32_t                           length{0};
    std::uint8_t                            seq{0};
};

// A length-prefixed codec for `message`. On the onboard typed lane encode() is NEVER invoked — the
// self-route delivers the object zero-copy by address. The shared atomic counts any encode call so
// the firmware can witness the zero-copy claim; a non-zero count is a real finding, not noise.
// encode()/decode() exist only to satisfy the typed_codec concept and the (unused) wire fallback.
struct message_codec
{
    using value_type = message;

    std::shared_ptr<std::atomic<int>> encodes = std::make_shared<std::atomic<int>>(0);

    plexus::wire_bytes<> encode(const message &v) const
    {
        ++*encodes;
        const std::size_t n     = v.length <= k_max_tier_bytes ? v.length : k_max_tier_bytes;
        auto              owner  = std::make_shared<std::array<std::byte, k_max_tier_bytes>>();
        for(std::size_t i = 0; i < n; ++i)
            (*owner)[i] = v.buffer[i];
        std::span<const std::byte> view{owner->data(), n};
        return plexus::wire_bytes<>{view, std::move(owner)};
    }

    plexus::expected<void, std::error_code> decode(std::span<const std::byte> bytes, message &out) const
    {
        if(bytes.size() > k_max_tier_bytes)
            return plexus::expected<void, std::error_code>{plexus::unexpect, std::make_error_code(std::errc::message_size)};
        out.length = static_cast<std::uint32_t>(bytes.size());
        for(std::size_t i = 0; i < bytes.size(); ++i)
            out.buffer[i] = bytes[i];
        out.seq = bytes.empty() ? 0 : static_cast<std::uint8_t>(bytes[0]);
        return {};
    }

    plexus::type_identity type_info() const
    {
        return {0x0B0ABEEFu, "onboard_message"};
    }
};

static_assert(plexus::typed_codec<message_codec>);

// One machine-parseable sample line on the console (UART0) the runner parses: fixed fields,
// space-separated key=value pairs under a stable BENCH tag, no localized formatting.
inline void emit_sample(const char *policy, std::size_t tier, std::uint32_t index, std::int64_t rtt_us)
{
    std::printf("BENCH sample policy=%s tier=%u index=%u rtt_us=%lld\n", policy, static_cast<unsigned>(tier), static_cast<unsigned>(index), static_cast<long long>(rtt_us));
}

// The zero-copy witness line: the codec's encode-call count over the whole sweep. On the intra-node
// typed lane this MUST be 0 — the headline proof that the self-route never serialized.
inline void emit_witness(const char *policy, int encodes)
{
    std::printf("BENCH witness policy=%s encodes=%d\n", policy, encodes);
}

inline void emit_resource(const char *policy, std::uint32_t free_heap)
{
    std::printf("BENCH resource policy=%s free_heap=%u\n", policy, static_cast<unsigned>(free_heap));
}

}

#endif
