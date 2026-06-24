#include "support/alloc_counter.h"

#include "in_memory_byte_sink.h"

#include "plexus/io/recording/dropout_record.h"
#include "plexus/io/recording/byte_ring.h"
#include "plexus/io/capture_policy.h"

#include "plexus/wire/varint.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <vector>
#include <cstddef>
#include <cstdint>

using namespace plexus::io::recording;
using plexus::io::capture_fidelity;

namespace {

std::vector<std::byte> record_of(std::size_t n, std::byte fill)
{
    return std::vector<std::byte>(n, fill);
}

// Decode the framed records the ring drained into the sink: [varint len][bytes]*,
// returning the count so a case can check sum(dropout)+drained==produced.
std::size_t decode_record_count(std::span<const std::byte> stream)
{
    std::size_t off   = 0;
    std::size_t count = 0;
    while(off < stream.size())
    {
        auto len = plexus::wire::read_varint(stream, off);
        if(!len)
            break;
        off += static_cast<std::size_t>(*len);
        ++count;
    }
    return count;
}

}

TEST_CASE("byte_ring allocates its backing store once and never reallocates", "[byte_ring]")
{
    byte_ring ring{4096};
    const auto rec = record_of(24, std::byte{0xAB});

    plexus::testing::reset_alloc_count();
    const std::size_t before = plexus::testing::alloc_count();
    for(int i = 0; i < 100000; ++i)
        (void)ring.try_push(rec);
    const std::size_t after = plexus::testing::alloc_count();

    REQUIRE(after == before);
}

TEST_CASE("byte_ring never blocks and drops-newest on a full ring", "[byte_ring]")
{
    byte_ring ring{256};
    const auto rec = record_of(40, std::byte{0x7E});

    bool saw_reject      = false;
    std::size_t admitted = 0;
    for(int i = 0; i < 1000; ++i)
    {
        if(ring.try_push(rec))
            ++admitted;
        else
            saw_reject = true;
    }

    REQUIRE(saw_reject);
    REQUIRE(ring.used() <= ring.capacity());

    in_memory_byte_sink sink;
    while(ring.drain(sink, 64))
    {
    }
    REQUIRE(decode_record_count(sink.bytes()) == admitted);
}

TEST_CASE("byte_ring accounts every shed record exactly (recall 1.0)", "[byte_ring][dropout]")
{
    for(int iteration = 0; iteration < 5; ++iteration)
    {
        byte_ring ring{512};
        dropout_run run;
        in_memory_byte_sink sink;

        const std::size_t produced = 5000;
        std::size_t drained        = 0;
        const auto rec             = record_of(48, std::byte{0x11});

        for(std::size_t i = 0; i < produced; ++i)
        {
            if(!ring.try_push(rec))
                run.shed(rec.size(), capture_fidelity::payload);

            if((i % 7) == 0)
            {
                ring.drain(sink, 128);
                drained = decode_record_count(sink.bytes());
            }
        }
        while(ring.drain(sink, 256))
        {
        }
        drained = decode_record_count(sink.bytes());

        const dropout_record gap = run.harvest(0);
        REQUIRE(gap.count + drained == produced);
        REQUIRE(gap.max_fidelity == capture_fidelity::payload);
    }
}

TEST_CASE("byte_ring never exceeds its byte budget under saturation", "[byte_ring]")
{
    const std::size_t budget = 1024;
    byte_ring ring{budget};
    const auto rec = record_of(60, std::byte{0x55});

    for(int i = 0; i < 100000; ++i)
    {
        (void)ring.try_push(rec);
        REQUIRE(ring.used() <= budget);
    }
}

TEST_CASE("byte_ring drains fully with no thread into a byte_sink", "[byte_ring]")
{
    byte_ring ring{4096};
    in_memory_byte_sink sink;

    const std::size_t produced = 30;
    for(std::size_t i = 0; i < produced; ++i)
        REQUIRE(ring.try_push(record_of(16 + i, std::byte{static_cast<unsigned char>(i)})));

    while(ring.drain(sink, 48))
    {
    }
    REQUIRE(ring.used() == 0);
    REQUIRE(decode_record_count(sink.bytes()) == produced);
}

TEST_CASE("byte_ring drop-oldest overwrites the oldest unread record", "[byte_ring]")
{
    byte_ring ring{200, ring_policy::drop_oldest};
    const auto rec = record_of(40, std::byte{0x33});

    for(int i = 0; i < 1000; ++i)
        REQUIRE(ring.try_push(rec));

    REQUIRE(ring.used() <= ring.capacity());

    in_memory_byte_sink sink;
    while(ring.drain(sink, 64))
    {
    }
    const std::size_t held = decode_record_count(sink.bytes());
    REQUIRE(held > 0);
    REQUIRE(held <= ring.capacity() / 41);
}
