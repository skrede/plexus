// The fast-path named gates: cell 6 (the zero-serialization identity witness, here as the
// named gate with the carrier-field assertions) and cell 7 (the LOOPED fast/fallback flip).
// The flip is proven the no-success-from-a-single-run way: a fresh net per iteration, the
// path of EVERY message witnessed (the codec's encode-count delta plus object-address
// identity vs value-only equality), every iteration asserted, and a proven-counter equal to
// the iteration count at the end. The single-dialer topology (subscriber eager, publisher
// lazy) matches the matrix fixture. The codec is hand-rolled — plexus never names a codec.

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

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <atomic>
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
using bytes_subscriber = plexus::subscriber<>;

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
    opts.redial_seed = 0xF11Du;
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
        REQUIRE(a.router().is_connected(id_b));
    }
};

}

TEST_CASE("typed fast path cell 6: the identity witness — same address, zero encodes, intra-process", "[node][typed][fastpath]")
{
    net n;
    n.connect();

    std::vector<const sample *> seen_addr;
    std::vector<std::uint32_t> seen_value;
    std::vector<message_info> infos;
    typed_subscriber s{n.a, "topic",
                       [&](const sample &v, const message_info &info) {
                           seen_addr.push_back(&v);
                           seen_value.push_back(v.value);
                           infos.push_back(info);
                       }};
    counting_codec codec;
    auto encodes = codec.encodes;
    typed_publisher p{n.b, "topic", plexus::typed_publisher_options{}, codec};
    n.drive();

    auto loan = p.borrow();
    REQUIRE(loan);
    loan->value = 0xABCDu;
    const sample *published_addr = &*loan;
    p.publish(std::move(loan));
    n.drive();

    REQUIRE(seen_value.size() == 1);
    REQUIRE(seen_value.front() == 0xABCDu);
    REQUIRE(seen_addr.front() == published_addr);   // the SAME object, by address
    REQUIRE(encodes->load() == 0);                  // the codec's encode was never invoked
    REQUIRE(infos.front().from_intra_process);
    REQUIRE(infos.front().publication_sequence == 0);   // the first publish on the topic
    REQUIRE(infos.front().reception_timestamp != 0);
    REQUIRE(infos.front().source_timestamp != 0);
    REQUIRE_FALSE(infos.front().source_identity.has_value());
}

// Cell 7, the looped flip. Each iteration stands up a FRESH net and witnesses the path of
// every message: a fast-path message arrives by the SAME object address with zero encode
// delta; a fallback message arrives value-equal with a NON-zero encode delta (and never by a
// live borrowed address). The iteration alternates between an eligible (typed, process-tier)
// subscriber — which fast-paths a borrow — and an ineligible (bytes) subscriber — which
// forces the byte path even for a borrowed loan; it also fires a pool-exhaustion burst that
// makes some publishes within one iteration fall back mid-stream.
TEST_CASE("typed fast path cell 7: the fast/fallback flip is looped, every message's path witnessed", "[node][typed][fastpath]")
{
    constexpr int k_iterations = 8;
    constexpr std::size_t k_pool_depth = 4;
    int proven = 0;

    for(int iter = 0; iter < k_iterations; ++iter)
    {
        const bool eligible = (iter % 2 == 0);

        net n;
        n.connect();

        // The witnesses the subscriber records, regardless of which kind it is.
        std::vector<std::uint32_t> values;
        std::vector<const void *> addrs;   // the delivered object address (fast path) or null

        counting_codec codec;
        auto encodes = codec.encodes;
        plexus::typed_publisher_options popts;
        popts.pool_depth = k_pool_depth;
        typed_publisher p{n.b, "topic", popts, codec};

        // Either an eligible typed subscriber (object fast path) or an ineligible bytes
        // subscriber (byte path always). Only one is alive per iteration so the path of a
        // borrowed publish is unambiguous.
        std::optional<typed_subscriber> ts;
        std::optional<bytes_subscriber> bs;
        if(eligible)
            ts.emplace(n.a, "topic",
                       [&](const sample &v) { values.push_back(v.value); addrs.push_back(&v); });
        else
            bs.emplace(n.a, "topic", [&](std::span<const std::byte> b) {
                std::uint32_t v = 0;
                for(int i = 0; i < 4 && i < static_cast<int>(b.size()); ++i)
                    v |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(b[i])) << (8 * i);
                values.push_back(v);
                addrs.push_back(nullptr);
            });
        n.drive();

        // A borrow-and-publish whose path is decided by the live subscriber kind. Returns
        // the borrowed object address (or null when the pool was exhausted and it serialized).
        auto borrow_publish = [&](std::uint32_t value) -> const void * {
            auto loan = p.borrow();
            if(!loan)
            {
                p.publish(sample{value});   // exhausted -> serialize path
                return nullptr;
            }
            loan->value = value;
            const void *addr = &*loan;
            p.publish(std::move(loan));
            return addr;
        };

        // One in-flight borrow per drive keeps the pool from exhausting, so the path is
        // purely the subscriber-kind flip (eligible typed = fast, ineligible bytes = byte).
        std::vector<const void *> expected_addr;
        std::vector<std::uint32_t> expected_value;
        for(int i = 0; i < 3; ++i)
        {
            const std::uint32_t value = 0x1000u + static_cast<std::uint32_t>(i);
            const int before = encodes->load();
            const void *addr = borrow_publish(value);
            n.drive();   // delivers + releases the slot before the next borrow
            const int delta = encodes->load() - before;

            expected_value.push_back(value);
            if(eligible)
            {
                REQUIRE(delta == 0);                  // fast path: no encode
                expected_addr.push_back(addr);        // delivered by the borrowed address
            }
            else
            {
                REQUIRE(delta == 1);                  // byte path: exactly one encode
                expected_addr.push_back(nullptr);
            }
        }

        // An eligible-iteration burst PAST the pool capacity within one drive window: the
        // pool has k_pool_depth slots and nothing drains until drive(), so the first
        // k_pool_depth borrows fast-path and the remainder fall back to serialize — a
        // fast/fallback flip WITHIN one iteration. (An ineligible iteration already
        // byte-paths every message, so the burst would add nothing to witness.)
        if(eligible)
        {
            const std::size_t burst = k_pool_depth + 3;
            std::vector<const void *> burst_addr;
            std::vector<std::uint32_t> burst_value;
            const int before = encodes->load();
            for(std::size_t i = 0; i < burst; ++i)
            {
                const std::uint32_t value = 0x2000u + static_cast<std::uint32_t>(i);
                burst_value.push_back(value);
                burst_addr.push_back(borrow_publish(value));   // no drive between -> pool drains down
            }
            n.drive();
            const int delta = encodes->load() - before;

            // The first k_pool_depth borrows held a slot (fast, null encode); the rest were
            // exhausted and serialized. The witnessed encode delta is exactly the overflow,
            // and loan_exhausted() agrees.
            REQUIRE(delta == static_cast<int>(burst - k_pool_depth));
            REQUIRE(p.loan_exhausted() == burst - k_pool_depth);
            for(std::size_t i = 0; i < burst; ++i)
            {
                expected_value.push_back(burst_value[i]);
                expected_addr.push_back(i < k_pool_depth ? burst_addr[i] : nullptr);
            }
        }

        // Every published message was delivered, in order, by the witnessed path. A
        // fast-path message is delivered by its borrowed object address; a fallback message
        // is delivered value-equal (a typed subscriber decodes into its own slot, a bytes
        // subscriber gets the encoding) — so address identity is asserted ONLY on the
        // fast-path messages, value equality on all.
        REQUIRE(values == expected_value);
        REQUIRE(addrs.size() == expected_addr.size());
        for(std::size_t i = 0; i < addrs.size(); ++i)
            if(expected_addr[i] != nullptr)
                REQUIRE(addrs[i] == expected_addr[i]);   // fast: the borrowed address

        ++proven;
    }

    REQUIRE(proven == k_iterations);
}
