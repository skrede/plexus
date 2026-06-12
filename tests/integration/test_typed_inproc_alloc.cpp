// The standing alloc-free gate for the typed in-process fast path: a warmed steady-state
// loop of borrow -> publish(loan&&) -> drain -> typed callback must allocate ZERO bytes.
// The fast path is the zero-serialization lane (the loan pool produces a slot, the object
// rides the bus by reference, the codec's encode is never invoked), so the only heap the
// steady loop could touch is the bus queue or the demux — both of which must be alloc-free
// after warm-up for the determinism invariant to hold. A forced-fallback variant documents
// (does NOT gate to zero) the serialize path's allocation behavior for contrast.
//
// The replaceable global new/delete (support/alloc_counter.h) constrains this to ONE TU per
// executable, so it is its own ctest binary.

#include "plexus/node.h"
#include "plexus/publisher.h"
#include "plexus/subscriber.h"
#include "plexus/expected.h"
#include "plexus/typed_codec.h"
#include "plexus/wire_bytes.h"
#include "plexus/node_options.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/io/message_info.h"
#include "plexus/io/endpoint_seam.h"

#include "support/alloc_counter.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <memory>
#include <vector>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <system_error>

namespace {

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_transport;
using plexus::discovery::static_discovery;
using plexus::io::message_info;

using inproc_node = plexus::node<inproc_policy, inproc_transport<>>;

struct sample
{
    std::uint32_t value{};
};

struct counting_codec
{
    using value_type = sample;

    std::shared_ptr<std::atomic<int>> encodes = std::make_shared<std::atomic<int>>(0);

    plexus::wire_bytes<> encode(const sample &v) const
    {
        ++*encodes;
        auto owner = std::make_shared<std::vector<std::byte>>(4);
        for(int i = 0; i < 4; ++i)
            (*owner)[i] = static_cast<std::byte>((v.value >> (8 * i)) & 0xff);
        std::span<const std::byte> view{owner->data(), owner->size()};
        return plexus::wire_bytes<>{view, std::move(owner)};
    }

    plexus::expected<void, std::error_code> decode(std::span<const std::byte> bytes, sample &out) const
    {
        if(bytes.size() != 4)
            return plexus::expected<void, std::error_code>{
                plexus::unexpect, std::make_error_code(std::errc::invalid_argument)};
        std::uint32_t v = 0;
        for(int i = 0; i < 4; ++i)
            v |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(bytes[i])) << (8 * i);
        out.value = v;
        return {};
    }

    plexus::type_identity type_info() const { return {0xABCD1234u, "sample"}; }
};

static_assert(plexus::typed_codec<counting_codec>);

using typed_publisher = plexus::publisher<counting_codec>;
using typed_subscriber = plexus::subscriber<counting_codec>;

plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

plexus::node_options make_opts(bool eager)
{
    plexus::node_options opts;
    opts.reconnect = plexus::io::reconnect_config{std::chrono::milliseconds(50),
                                                  std::chrono::milliseconds(2000),
                                                  std::nullopt, std::nullopt};
    opts.redial_seed = 0xA110Cu;
    opts.dial_eagerly = eager;
    return opts;
}

struct net
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    inproc_transport<> ta{ex, bus};
    inproc_transport<> tb{ex, bus};
    static_discovery disc{{}};

    plexus::node_id id_a{make_id(0x0A)};
    plexus::node_id id_b{make_id(0x0B)};

    inproc_node a{ex, disc, id_a, ta, make_opts(/*eager=*/true)};
    inproc_node b{ex, disc, id_b, tb, make_opts(/*eager=*/false)};

    void drive() { ex.drain(); }

    void connect()
    {
        a.listen({"inproc", "host-a:5000"});
        b.listen({"inproc", "host-b:6000"});
        drive();
    }
};

}

TEST_CASE("typed inproc fast path: the warmed steady-state borrow->publish->drain loop is zero-alloc", "[integration]")
{
    constexpr int warm = 64;     // cycle the pool slots, the bus deque blocks, and the demux
    constexpr int K = 4096;      // the measured steady-state loop

    net n;
    n.connect();

    std::uint32_t last = 0;
    std::size_t   delivered = 0;
    typed_subscriber s{n.a, "topic",
                       [&](const sample &v) { last = v.value; ++delivered; }};
    counting_codec codec;
    auto encodes = codec.encodes;
    typed_publisher p{n.b, "topic", plexus::typed_publisher_options{}, codec};
    n.drive();

    auto fast_publish = [&](std::uint32_t value) {
        auto loan = p.borrow();
        REQUIRE(loan);
        loan->value = value;
        p.publish(std::move(loan));
        n.drive();
    };

    // Warm-up: grow every first-touch buffer (the bus deque block, the demux info stack POD
    // touches nothing heap, the pool slots are grown at construction) so the measured window
    // is pure steady state.
    for(int i = 0; i < warm; ++i)
        fast_publish(static_cast<std::uint32_t>(i));

    const std::size_t delivered_before = delivered;
    const int encodes_before = encodes->load();

    plexus::testing::reset_alloc_count();
    const auto before = plexus::testing::alloc_count();
    for(int i = 0; i < K; ++i)
        fast_publish(0xF0000000u + static_cast<std::uint32_t>(i));
    const auto after = plexus::testing::alloc_count();

    REQUIRE(delivered - delivered_before == static_cast<std::size_t>(K));   // every publish delivered
    REQUIRE(encodes->load() == encodes_before);                            // the fast path never encoded
    REQUIRE(last == 0xF0000000u + static_cast<std::uint32_t>(K - 1));
    REQUIRE(after - before == 0);   // the zero-serialization steady-state loop allocated nothing
}

TEST_CASE("typed inproc forced fallback: the serialize path's allocation is documented, not gated to zero", "[integration]")
{
    constexpr int warm = 64;
    constexpr int K = 1024;

    net n;
    n.connect();

    std::size_t delivered = 0;
    typed_subscriber s{n.a, "topic", [&](const sample &) { ++delivered; }};
    counting_codec codec;
    auto encodes = codec.encodes;
    // A depth-0 pool forces every publish(const T&) onto the serialize path: encode the
    // value and publish bytes. This is the byte fallback the fast path degrades to under
    // exhaustion; its allocation is REPORTED for contrast, not asserted to zero.
    plexus::typed_publisher_options popts;
    popts.pool_depth = 0;
    typed_publisher p{n.b, "topic", popts, codec};
    n.drive();

    for(int i = 0; i < warm; ++i)
    {
        p.publish(sample{static_cast<std::uint32_t>(i)});
        n.drive();
    }

    const int encodes_before = encodes->load();
    const std::size_t delivered_before = delivered;

    plexus::testing::reset_alloc_count();
    const auto before = plexus::testing::alloc_count();
    for(int i = 0; i < K; ++i)
    {
        p.publish(sample{0xE0000000u + static_cast<std::uint32_t>(i)});
        n.drive();
    }
    const auto after = plexus::testing::alloc_count();

    REQUIRE(delivered - delivered_before == static_cast<std::size_t>(K));   // delivered via bytes
    REQUIRE(encodes->load() - encodes_before == K);                         // every publish encoded
    REQUIRE(p.loan_exhausted() == static_cast<std::size_t>(warm + K));      // the pool degraded every time
    // The fallback path allocates (the codec mints a fresh owner buffer per encode); this is
    // the documented contrast to the fast path's zero, not a gated invariant.
    SUCCEED("forced-fallback steady-loop allocations recorded: " + std::to_string(after - before));
}

// The license for the encode-erasure seam: constructing an io::encode_thunk per publish
// over a multi-pointer-capturing lambda must allocate ZERO. The thunk binds the lambda by
// reference (state = &lambda) and dispatches through a captureless trampoline, so the
// erased encode is a two-word POD with no heap regardless of capture size — the same gate
// the seam refactor will rely on when publisher.h constructs the thunk per publish.
TEST_CASE("encode-thunk construction over a multi-pointer capture is zero-alloc", "[integration]")
{
    constexpr int warm = 64;
    constexpr int K = 4096;

    std::array<std::byte, 4> payload{};
    std::uint64_t            sequence = 0;
    std::uint32_t            value = 0;

    auto build_and_invoke = [&](std::uint32_t v) -> std::size_t {
        value = v;
        ++sequence;
        // The encode lambda captures THREE pointers (payload, sequence, value) by
        // reference — a capture wider than any small-buffer would inline — so a heap
        // erasure would show here. The thunk must still allocate nothing.
        auto encode = [&payload, &sequence, &value]() -> std::span<const std::byte> {
            payload[0] = static_cast<std::byte>(value & 0xff);
            payload[1] = static_cast<std::byte>((value >> 8) & 0xff);
            payload[2] = static_cast<std::byte>(sequence & 0xff);
            payload[3] = static_cast<std::byte>((sequence >> 8) & 0xff);
            return std::span<const std::byte>{payload};
        };
        plexus::io::encode_thunk thunk = plexus::io::make_encode_thunk(encode);
        return plexus::io::invoke(thunk).size();
    };

    std::size_t sink = 0;
    for(int i = 0; i < warm; ++i)
        sink += build_and_invoke(static_cast<std::uint32_t>(i));

    plexus::testing::reset_alloc_count();
    const auto before = plexus::testing::alloc_count();
    for(int i = 0; i < K; ++i)
        sink += build_and_invoke(0xD0000000u + static_cast<std::uint32_t>(i));
    const auto after = plexus::testing::alloc_count();

    REQUIRE(sink == static_cast<std::size_t>((warm + K) * 4));
    REQUIRE(after - before == 0);   // erased encode construction allocated nothing
}
