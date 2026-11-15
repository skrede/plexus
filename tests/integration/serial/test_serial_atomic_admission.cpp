#include "test_serial_atomic_admission_common.h"

#include "support/serial_bytes.h"

#include "plexus/asio/detail/serial_bootstrap.h"

#include "plexus/wire_bytes.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <memory>
#include <vector>
#include <cstddef>
#include <utility>

using namespace serial_admission_fixture;

TEST_CASE("serial submit admits header, payload and CRC trailer as one well-formed unit", "[integration][serial][atomic_admission]")
{
    // A byte cap of exactly the framed-message size: separate admissions would admit the header and
    // payload and reject the trailer, tearing the frame. The atomic admission treats the three nodes
    // as one message and admits them onto the empty queue — one verifiable serial frame.
    const auto frame = framed_message("atomic-serial-payload");

    rec_sink rec;
    mock_channel ch{rec.sink(), frame.size()};
    plexus::asio::detail::serial_bootstrap<int> boot;

    std::shared_ptr<std::vector<std::byte>> keep;
    boot.submit(ch, owned_of(frame, keep));

    REQUIRE(ch.full_events == 0);
    REQUIRE(rec.turns.size() == 1);
    REQUIRE(rec.views_per_turn[0] == 3);
    const auto wire = rec.wire_stream();
    REQUIRE(wire == plexus::testing::serial_bytes(frame));
    const auto seen = verify_wire(wire);
    REQUIRE(seen.matches == 1);
    REQUIRE(seen.drops == 0);
    REQUIRE(seen.last_match == frame);
}

TEST_CASE("serial submit admits the header+payload+trailer unit through the borrowed-view overload", "[integration][serial][atomic_admission]")
{
    const auto frame = framed_message("borrowed-view-payload");

    rec_sink rec;
    mock_channel ch{rec.sink(), frame.size()};
    plexus::asio::detail::serial_bootstrap<int> boot;

    boot.submit(ch, std::span<const std::byte>{frame});

    REQUIRE(ch.full_events == 0);
    REQUIRE(rec.views_per_turn.at(0) == 3);
    const auto seen = verify_wire(rec.wire_stream());
    REQUIRE(seen.matches == 1);
    REQUIRE(seen.drops == 0);
    REQUIRE(seen.last_match == frame);
}

TEST_CASE("serial submit is all-or-nothing on a full queue and never reads the moved-from payload", "[integration][serial][atomic_admission]")
{
    // With the queue full the unit is rejected while the payload's owner is still held, so the CRCs
    // never read a freed view (ASan-clean) and all three nodes are refused together.
    const auto frame = framed_message("rejected-under-backpressure");

    rec_sink rec;
    mock_channel ch{rec.sink(), frame.size()};
    plexus::asio::detail::serial_bootstrap<int> boot;

    // Saturate the queue: a first frame fills the cap and parks in flight (its completion is held).
    std::vector<std::byte> filler(frame.size());
    ch.enqueue_egress(filler);
    REQUIRE(ch.queue.full());
    REQUIRE(rec.turns.size() == 1);

    // The payload's SOLE owner is the wire_bytes handed to submit, so a rejected move-in frees it.
    auto owner = std::make_shared<std::vector<std::byte>>(frame.begin(), frame.end());
    plexus::wire_bytes<> payload{std::span<const std::byte>{*owner}, owner};
    owner.reset();
    boot.submit(ch, std::move(payload));

    REQUIRE(ch.full_events == 1);
    REQUIRE(rec.turns.size() == 1);
    REQUIRE(ch.queue.size() == 1);
}
