#include "support/alloc_counter.h"

#include "plexus/io/splice_pool.h"
#include "plexus/io/forward_options.h"

#include "plexus/wire/forwarded_frame.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <vector>
#include <cstddef>

namespace {

plexus::wire::forwarded_frame sample_envelope()
{
    plexus::wire::forwarded_frame ff{};
    ff.origin.fill(std::byte{0x11});
    ff.destination.fill(std::byte{0x22});
    ff.hop = 1;
    ff.seq = 7;
    ff.inner.assign(64, std::byte{0xAB});
    return ff;
}

template<typename Build>
std::size_t per_iteration_alloc_delta(Build &&build)
{
    constexpr int k_iterations = 256;
    build(); // prime: the pool warms, the owner's control block is minted once here
    plexus::testing::reset_alloc_count();
    const auto before = plexus::testing::alloc_count();
    plexus::wire_bytes<> held;
    for(int i = 0; i < k_iterations; ++i)
        held = build();
    const auto after = plexus::testing::alloc_count();
    return (after - before) / k_iterations;
}

}

TEST_CASE("forward_splice_alloc: the pooled buffer-build allocates nothing in steady state", "[forwarder]")
{
    const auto ff = sample_envelope();

    plexus::io::forward_options opts;
    opts.splice_pool_slots  = 4;
    opts.splice_slot_bytes  = 256;
    const auto ctx = plexus::io::make_forward_ctx(opts);
    plexus::io::splice_pool pool{ctx.splice_pool_slots, ctx.splice_slot_bytes};

    // The shape the pool replaces: a fresh owning buffer per publish — the make_shared placeholder the
    // zero-steady-state-alloc gate forbids on the relay splice. Pinned as a durable contrast so the
    // pooled build below cannot silently regress to it.
    const auto naive_build = [&]
    {
        auto buf = std::make_shared<std::vector<std::byte>>(plexus::wire::encode_forwarded_frame(ff));
        std::span<const std::byte> view{*buf};
        return plexus::wire_bytes<>{view, std::shared_ptr<const void>{std::move(buf)}};
    };
    REQUIRE(per_iteration_alloc_delta(naive_build) > 0);

    // Pooled owned copy (D95.1 default): checkout claims a free slot, the envelope encodes into it, and
    // the returned wire_bytes<> returns the slot to the pool on release — zero per-iteration alloc.
    const auto pool_build = [&]
    {
        return pool.checkout_owned_copy(
                [&](std::span<std::byte> slot)
                { return plexus::wire::encode_forwarded_frame_into(slot, ff); });
    };
    REQUIRE(per_iteration_alloc_delta(pool_build) == 0);
}

TEST_CASE("forward_splice_alloc: the refcounted zero-copy checkout retains the owner without allocating", "[forwarder]")
{
    plexus::io::splice_pool pool{4, 256};

    // The owner-carrying inbound view the zero-copy opt-in retains; its control block is minted once
    // here, so wrapping it each iteration copies only the span and addrefs the owner — no slot copy.
    plexus::wire::shared_bytes owner{std::vector<std::byte>(128, std::byte{0xCD})};

    const auto zero_copy_build = [&] { return pool.checkout_zero_copy(owner); };

    REQUIRE(per_iteration_alloc_delta(zero_copy_build) == 0);
    REQUIRE_FALSE(zero_copy_build().empty());
}

TEST_CASE("forward_splice_alloc: pool exhaustion counts a drop and allocates nothing", "[forwarder]")
{
    const auto ff = sample_envelope();
    plexus::io::splice_pool pool{2, 256};

    const auto enc = [&](std::span<std::byte> slot)
    { return plexus::wire::encode_forwarded_frame_into(slot, ff); };

    plexus::wire_bytes<> a = pool.checkout_owned_copy(enc);
    plexus::wire_bytes<> b = pool.checkout_owned_copy(enc);
    REQUIRE_FALSE(a.empty());
    REQUIRE_FALSE(b.empty());

    const auto drops_before = pool.exhaustion_drops();
    plexus::testing::reset_alloc_count();
    const auto before = plexus::testing::alloc_count();
    plexus::wire_bytes<> exhausted = pool.checkout_owned_copy(enc);
    const auto after = plexus::testing::alloc_count();

    REQUIRE(exhausted.empty());
    REQUIRE(after == before);
    REQUIRE(pool.exhaustion_drops() == drops_before + 1);
    REQUIRE(plexus::io::splice_pool::exhaustion_cause() == plexus::io::detail::drop_cause::splice_exhausted);
}
