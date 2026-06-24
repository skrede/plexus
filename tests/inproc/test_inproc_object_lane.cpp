#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_timer.h"
#include "plexus/inproc/inproc_bus.h"

#include "plexus/io/object_carrier.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <vector>
#include <cstdint>
#include <cstddef>

using namespace plexus::inproc;
using plexus::io::loan_slot;
using plexus::io::object_carrier;

namespace {

std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

// A pool-slot stand-in with a counting release: release_calls increments every time
// the slot drops to zero. A balanced run ends with refs == 0 and release_calls == 1.
struct counted_payload
{
    int       value{0};
    int       release_calls{0};
    loan_slot slot{};
};

object_carrier make_carrier(counted_payload &p, std::uint64_t topic, std::uint64_t tag, std::uint64_t seq = 0)
{
    p.slot.object  = &p.value;
    p.slot.refs    = 0;
    p.slot.release = [](loan_slot *s)
    {
        auto *owner = reinterpret_cast<counted_payload *>(reinterpret_cast<std::byte *>(s) - offsetof(counted_payload, slot));
        ++owner->release_calls;
    };
    return object_carrier{topic, tag, &p.value, seq, 0, &p.slot};
}

struct pair_fixture
{
    inproc_bus<>      bus;
    inproc_executor<> ex{bus};
    inproc_channel<>  a{ex};
    inproc_channel<>  b{ex};

    pair_fixture()
    {
        a.connect_to(b.local_endpoint());
        b.connect_to(a.local_endpoint());
    }
};

}

TEST_CASE("inproc object lane delivers the carrier to the partner with field fidelity", "[inproc][object]")
{
    pair_fixture    fx;
    counted_payload p;
    p.value = 42;

    object_carrier received{};
    bool           got = false;
    fx.b.on_object(
            [&](const object_carrier &c)
            {
                received = c;
                got      = true;
                plexus::io::release(c); // the receiving handler owns the delivered reference
            });

    fx.a.send_object(make_carrier(p, 0xABCDu, 0x1111u, 7));
    REQUIRE_FALSE(got); // never synchronous from send_object()

    fx.ex.drain();
    REQUIRE(got);
    REQUIRE(received.topic_hash == 0xABCDu);
    REQUIRE(received.type_tag == 0x1111u);
    REQUIRE(received.sequence == 7u);
    REQUIRE(received.slot == &p.slot);
    REQUIRE(*reinterpret_cast<const int *>(received.slot->object) == 42);

    REQUIRE(p.slot.refs == 0u);
    REQUIRE(p.release_calls == 1);
}

TEST_CASE("inproc object lane preserves FIFO ordering with interleaved byte packets", "[inproc][object]")
{
    pair_fixture    fx;
    counted_payload p0, p1;

    std::vector<std::string> order;
    fx.b.on_data([&](std::span<const std::byte> d) { order.emplace_back(reinterpret_cast<const char *>(d.data()), d.size()); });
    fx.b.on_object(
            [&](const object_carrier &c)
            {
                order.push_back("obj:" + std::to_string(c.sequence));
                plexus::io::release(c);
            });

    fx.a.send(as_bytes(std::string{"first"}));
    fx.a.send_object(make_carrier(p0, 1, 1, 100));
    fx.a.send(as_bytes(std::string{"third"}));
    fx.a.send_object(make_carrier(p1, 1, 1, 200));

    fx.ex.drain();

    REQUIRE(order == std::vector<std::string>{"first", "obj:100", "third", "obj:200"});
    REQUIRE(p0.release_calls == 1);
    REQUIRE(p1.release_calls == 1);
    REQUIRE(p0.slot.refs == 0u);
    REQUIRE(p1.slot.refs == 0u);
}

TEST_CASE("inproc object lane releases on a closed partner with no handler fire", "[inproc][object]")
{
    pair_fixture    fx;
    counted_payload p;

    bool fired = false;
    fx.b.on_object([&](const object_carrier &) { fired = true; });
    fx.b.close();

    fx.a.send_object(make_carrier(p, 1, 1, 1));
    fx.ex.drain();

    REQUIRE_FALSE(fired);       // a closed channel does not invoke its handler
    REQUIRE(p.slot.refs == 0u); // but the reference is still released
    REQUIRE(p.release_calls == 1);
}

TEST_CASE("inproc object lane releases when the target endpoint has vanished", "[inproc][object]")
{
    inproc_bus<>      bus;
    inproc_executor<> ex{bus};
    counted_payload   p;

    {
        inproc_channel<> a{ex};
        inproc_channel<> b{ex};
        a.connect_to(b.local_endpoint());
        // Enqueue toward b, then let b deregister before the step-loop delivers.
        a.send_object(make_carrier(p, 1, 1, 1));
        REQUIRE(p.slot.refs == 1u); // the bus holds its reference
    } // b (and a) destruct: the queued packet's target endpoint no longer exists

    ex.drain(); // deliver_one walks the now-empty channel set and must release
    REQUIRE(p.slot.refs == 0u);
    REQUIRE(p.release_calls == 1);
}

TEST_CASE("inproc object lane balances refcounts across a multi-packet burst", "[inproc][object]")
{
    pair_fixture fx;

    int fires = 0;
    fx.b.on_object(
            [&](const object_carrier &c)
            {
                ++fires;
                plexus::io::release(c);
            });

    constexpr int                k_burst = 32;
    std::vector<counted_payload> payloads(k_burst);
    for(int i = 0; i < k_burst; ++i)
        fx.a.send_object(make_carrier(payloads[i], 1, 1, static_cast<std::uint64_t>(i)));

    fx.ex.drain();

    REQUIRE(fires == k_burst);
    for(const auto &p : payloads)
    {
        REQUIRE(p.slot.refs == 0u);
        REQUIRE(p.release_calls == 1);
    }
}
