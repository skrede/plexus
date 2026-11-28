#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_timer.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"

#include "plexus/io/object_carrier.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <cstddef>
#include <cstdint>
#include <utility>

using namespace plexus::inproc;
using plexus::io::loan_slot;
using plexus::io::object_carrier;

namespace {

// The bus's ring starts with this many slots, so queuing as many packets leaves it full at the first delivery.
constexpr std::int32_t k_first_ring_capacity = 8;

std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

std::string as_string(std::span<const std::byte> d)
{
    return {reinterpret_cast<const char *>(d.data()), d.size()};
}

// A pool-slot stand-in whose release counts each time the slot drops to zero references.
struct counted_payload
{
    std::int32_t value{0};
    std::int32_t release_calls{0};
    loan_slot slot{};
};

object_carrier make_carrier(counted_payload &p)
{
    p.slot.object  = &p.value;
    p.slot.refs    = 0;
    p.slot.release = [](loan_slot *s)
    {
        auto *owner = reinterpret_cast<counted_payload *>(reinterpret_cast<std::byte *>(s) - offsetof(counted_payload, slot));
        ++owner->release_calls;
    };
    return object_carrier{1, 1, &p.value, 0, 0, &p.slot};
}

struct pair_fixture
{
    inproc_bus<> bus;
    inproc_executor<> ex;
    inproc_channel<> a;
    inproc_channel<> b;

    pair_fixture()
            : ex{bus}
            , a{ex}
            , b{ex}
    {
        a.connect_to(b.local_endpoint());
        b.connect_to(a.local_endpoint());
    }

    void fill_ring_and_drain()
    {
        for(std::int32_t i = 0; i < k_first_ring_capacity; ++i)
            a.send(as_bytes("packet-" + std::to_string(i)));
        ex.drain();
    }
};

std::string delivered_after_sends(pair_fixture &fx, std::int32_t sends)
{
    std::string seen;
    fx.b.on_data(
            [&](std::span<const std::byte> d)
            {
                if(!seen.empty())
                    return;
                const std::string reply(d.size(), 'r');
                for(std::int32_t i = 0; i < sends; ++i)
                    fx.b.send(as_bytes(reply));
                seen = as_string(d);
            });
    fx.fill_ring_and_drain();
    return seen;
}

}

TEST_CASE("inproc reentrant send: a same-size send from a delivery callback on a full ring leaves the delivered bytes intact", "[inproc][reentrant]")
{
    pair_fixture fx;
    REQUIRE(delivered_after_sends(fx, 1) == "packet-0");
}

TEST_CASE("inproc reentrant send: two sends from a delivery callback that grow a full ring leave the delivered bytes intact", "[inproc][reentrant]")
{
    pair_fixture fx;
    REQUIRE(delivered_after_sends(fx, 2) == "packet-0");
}

TEST_CASE("inproc reentrant send: an object sent from a delivery callback on a full ring is delivered and released once", "[inproc][reentrant]")
{
    pair_fixture fx;
    counted_payload p;
    bool sent      = false;
    bool delivered = false;
    fx.a.on_object(
            [&](const object_carrier &c)
            {
                delivered = c.slot == &p.slot;
                plexus::io::release(c);
            });
    fx.b.on_data(
            [&](std::span<const std::byte>)
            {
                if(!std::exchange(sent, true))
                    fx.b.send_object(make_carrier(p));
            });

    fx.fill_ring_and_drain();
    CHECK(delivered);
    CHECK(p.slot.refs == 0u);
    CHECK(p.release_calls == 1);
}
