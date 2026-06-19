// The handshake_gate oracle: a pure sans-IO drive of the open-before-data buffer with a
// recording drain callback (no socket, no crypto, no backend link — plexus::plexus only).
// It proves the buffer/drain/passthrough contract the crypto channel relied on:
// enqueue-before-ready buffers (the drain sees nothing until mark_ready); mark_ready
// drains all buffered nodes IN FIFO order; submit-after-ready passes straight through;
// reset() clears a pending buffer; and copy-into-owned-node (mutating the source after
// submit leaves the drained bytes unchanged).

#include "plexus/io/detail/handshake_gate.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <vector>
#include <cstddef>

using handshake_gate = plexus::io::detail::handshake_gate;

namespace {

// A recording drain: snapshots each presented bytes view into an owned copy so the test
// can assert what (and in what order) the gate forwarded.
struct recorder
{
    std::vector<std::vector<std::byte>> drained;

    handshake_gate::drain_fn drain()
    {
        return [this](std::span<const std::byte> bytes)
        { drained.emplace_back(bytes.begin(), bytes.end()); };
    }
};

std::vector<std::byte> bytes_of(std::initializer_list<int> vals)
{
    std::vector<std::byte> out;
    for(int v : vals)
        out.push_back(static_cast<std::byte>(v));
    return out;
}

}

TEST_CASE("handshake_gate buffers submissions before ready (the drain sees nothing)",
          "[io][handshake_gate]")
{
    recorder       rec;
    handshake_gate gate{rec.drain()};

    gate.submit(bytes_of({1}));
    gate.submit(bytes_of({2}));

    REQUIRE_FALSE(gate.is_ready());
    REQUIRE(gate.buffered() == 2);
    REQUIRE(rec.drained.empty()); // nothing drained until the ready edge
}

TEST_CASE("handshake_gate mark_ready drains the buffer in FIFO order then flips ready",
          "[io][handshake_gate]")
{
    recorder       rec;
    handshake_gate gate{rec.drain()};

    gate.submit(bytes_of({10}));
    gate.submit(bytes_of({20}));
    gate.submit(bytes_of({30}));

    gate.mark_ready();

    REQUIRE(gate.is_ready());
    REQUIRE(gate.buffered() == 0);
    REQUIRE(rec.drained.size() == 3);
    REQUIRE(rec.drained[0] == bytes_of({10}));
    REQUIRE(rec.drained[1] == bytes_of({20}));
    REQUIRE(rec.drained[2] == bytes_of({30}));
}

TEST_CASE("handshake_gate passes submissions straight through after ready", "[io][handshake_gate]")
{
    recorder       rec;
    handshake_gate gate{rec.drain()};

    gate.mark_ready(); // ready with an empty buffer: a no-op drain
    REQUIRE(rec.drained.empty());

    gate.submit(bytes_of({7})); // no buffering past the ready edge
    REQUIRE(gate.buffered() == 0);
    REQUIRE(rec.drained.size() == 1);
    REQUIRE(rec.drained[0] == bytes_of({7}));
}

TEST_CASE("handshake_gate reset clears a pending buffer", "[io][handshake_gate]")
{
    recorder       rec;
    handshake_gate gate{rec.drain()};

    gate.submit(bytes_of({1}));
    gate.submit(bytes_of({2}));
    REQUIRE(gate.buffered() == 2);

    gate.reset();
    REQUIRE(gate.buffered() == 0);

    // A subsequent mark_ready has nothing to drain (the buffer was dropped).
    gate.mark_ready();
    REQUIRE(rec.drained.empty());
}

TEST_CASE("handshake_gate copies submissions into an owned node", "[io][handshake_gate]")
{
    recorder       rec;
    handshake_gate gate{rec.drain()};

    auto scratch = bytes_of({1, 2, 3});
    gate.submit(scratch);

    // Mutate the caller's scratch AFTER submit: the buffered node holds its own copy, so
    // the drained bytes are unaffected (the non-owning-buffer hazard closed).
    scratch[0] = static_cast<std::byte>(99);

    gate.mark_ready();
    REQUIRE(rec.drained.size() == 1);
    REQUIRE(rec.drained[0] == bytes_of({1, 2, 3}));
}
