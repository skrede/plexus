// The head-of-line reorder oracle (D-04): the receiver delivers reliable segments in
// PUBLISH ORDER. A gap at seq N holds N+1.. in the buffer until N arrives, then the
// contiguous run releases in order. This is the load-bearing in-order proof — the gap
// case (feed 0, then 2, then 3, assert NOTHING past 0 delivers; feed 1, assert 1,2,3
// release in order) is asserted directly. The buffer is sans-IO (no socket, no
// io_context): a pure deterministic unit. Bounded + dup-drop + uint16 wrap are covered.

#include "plexus/io/detail/udp_reorder_buffer.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>

namespace pio = plexus::io;

namespace {

using reorder = pio::detail::udp_reorder_buffer;

std::vector<std::byte> bytes_of(const std::string &s)
{
    std::vector<std::byte> out(s.size());
    for(std::size_t i = 0; i < s.size(); ++i)
        out[i] = static_cast<std::byte>(s[i]);
    return out;
}

std::string str_of(std::span<const std::byte> b)
{
    std::string s(b.size(), '\0');
    for(std::size_t i = 0; i < b.size(); ++i)
        s[i] = static_cast<char>(std::to_integer<unsigned char>(b[i]));
    return s;
}

// A reorder buffer wired to record the in-order release sequence + payloads.
struct recorder
{
    reorder buf;
    std::vector<std::uint16_t> released_seq;
    std::vector<std::string> released_payload;

    explicit recorder(std::size_t window = reorder::default_window, std::uint16_t initial_seq = 0)
        : buf(window, initial_seq)
    {
        buf.on_deliver([this](std::uint16_t seq, std::span<const std::byte> b) {
            released_seq.push_back(seq);
            released_payload.push_back(str_of(b));
        });
    }
};

}

TEST_CASE("udp reorder: contiguous arrival delivers immediately in order", "[udp][reorder]")
{
    recorder r;
    REQUIRE(r.buf.feed(0, bytes_of("a")) == reorder::outcome::delivered);
    REQUIRE(r.buf.feed(1, bytes_of("b")) == reorder::outcome::delivered);
    REQUIRE(r.buf.feed(2, bytes_of("c")) == reorder::outcome::delivered);

    REQUIRE(r.released_seq == std::vector<std::uint16_t>{0, 1, 2});
    REQUIRE(r.released_payload == std::vector<std::string>{"a", "b", "c"});
    REQUIRE(r.buf.expected() == 3);
    REQUIRE(r.buf.cumulative() == 2);
}

TEST_CASE("udp reorder HOL (D-04): a gap holds all higher seqs until the gap fills",
          "[udp][reorder]")
{
    recorder r;

    // Feed 0 -> released. Then feed 2, 3 ahead of the gap at 1: NOTHING past 0 may
    // release (head-of-line blocking). They are buffered, not delivered.
    REQUIRE(r.buf.feed(0, bytes_of("zero")) == reorder::outcome::delivered);
    REQUIRE(r.buf.feed(2, bytes_of("two")) == reorder::outcome::buffered);
    REQUIRE(r.buf.feed(3, bytes_of("three")) == reorder::outcome::buffered);

    REQUIRE(r.released_seq == std::vector<std::uint16_t>{0});   // ONLY 0 so far
    REQUIRE(r.buf.expected() == 1);                              // still waiting on 1
    REQUIRE(r.buf.buffered_at(1));                               // hole offset 1 -> seq 2 buffered
    REQUIRE(r.buf.buffered_at(2));                               // hole offset 2 -> seq 3 buffered

    // The missing seq 1 arrives (a retransmit): the contiguous run 1,2,3 releases in
    // order in a single drain.
    REQUIRE(r.buf.feed(1, bytes_of("one")) == reorder::outcome::delivered);
    REQUIRE(r.released_seq == std::vector<std::uint16_t>{0, 1, 2, 3});
    REQUIRE(r.released_payload == std::vector<std::string>{"zero", "one", "two", "three"});
    REQUIRE(r.buf.expected() == 4);
}

TEST_CASE("udp reorder: a duplicate below expected is dropped, not re-delivered", "[udp][reorder]")
{
    recorder r;
    REQUIRE(r.buf.feed(0, bytes_of("a")) == reorder::outcome::delivered);
    REQUIRE(r.buf.feed(1, bytes_of("b")) == reorder::outcome::delivered);

    // Re-feeding an already-delivered seq is a duplicate -> dropped, on_deliver silent.
    REQUIRE(r.buf.feed(0, bytes_of("a")) == reorder::outcome::duplicate);
    REQUIRE(r.buf.feed(1, bytes_of("b")) == reorder::outcome::duplicate);
    REQUIRE(r.released_seq == std::vector<std::uint16_t>{0, 1});

    // A re-fed buffered hole (ahead of a gap, already present) is reported buffered,
    // not re-delivered.
    REQUIRE(r.buf.feed(3, bytes_of("d")) == reorder::outcome::buffered);
    REQUIRE(r.buf.feed(3, bytes_of("d")) == reorder::outcome::buffered);
    REQUIRE(r.released_seq == std::vector<std::uint16_t>{0, 1});
}

TEST_CASE("udp reorder: a seq at or beyond expected+W is out of window and dropped",
          "[udp][reorder]")
{
    recorder r{/*window=*/4};
    REQUIRE(r.buf.feed(0, bytes_of("a")) == reorder::outcome::delivered);   // expected -> 1

    // Window is 4: holes at offsets [0,4) from expected=1 are admissible (seq 1..4).
    REQUIRE(r.buf.feed(4, bytes_of("ok")) == reorder::outcome::buffered);   // offset 3, in window
    REQUIRE(r.buf.feed(5, bytes_of("no")) == reorder::outcome::out_of_window);   // offset 4, past bound
    REQUIRE(r.released_seq == std::vector<std::uint16_t>{0});
}

TEST_CASE("udp reorder: in-order delivery survives the uint16 65535 -> 0 wrap", "[udp][reorder]")
{
    recorder r{reorder::default_window, /*initial_seq=*/65534};

    REQUIRE(r.buf.feed(65534, bytes_of("x")) == reorder::outcome::delivered);
    REQUIRE(r.buf.feed(65535, bytes_of("y")) == reorder::outcome::delivered);
    REQUIRE(r.buf.feed(0, bytes_of("z")) == reorder::outcome::delivered);     // the wrap is a forward step
    REQUIRE(r.buf.feed(1, bytes_of("w")) == reorder::outcome::delivered);

    REQUIRE(r.released_seq == std::vector<std::uint16_t>{65534, 65535, 0, 1});
    REQUIRE(r.released_payload == std::vector<std::string>{"x", "y", "z", "w"});

    // A hole across the wrap is held then released in order: deliver up to 2, gap at 3,
    // buffer 4 across no wrap here — exercise a gap right after the wrap.
    REQUIRE(r.buf.feed(3, bytes_of("ahead")) == reorder::outcome::buffered);  // gap at 2
    REQUIRE(r.released_seq.size() == 4);
    REQUIRE(r.buf.feed(2, bytes_of("fill")) == reorder::outcome::delivered);
    REQUIRE(r.released_seq == std::vector<std::uint16_t>{65534, 65535, 0, 1, 2, 3});
}
