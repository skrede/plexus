#include "test_stream_inbound_common.h"

#include "support/race_timer.h"

#include <span>
#include <vector>
#include <cstddef>

using namespace stream_inbound_fixture;

using plexus::testing::race_timer;
using plexus::testing::race_executor;

namespace {

using race_stream = stream_inbound<race_timer, race_executor &>;

struct race_fixture
{
    race_executor ex;
    counters c;
    race_stream stream{ex, test_config()};

    race_fixture()
    {
        stream.on_frame([this](const complete_frame &) { ++c.frames; });
        stream.on_protocol_close(
                [this](close_cause cause)
                {
                    ++c.closes;
                    c.last_cause = cause;
                });
    }

    race_timer &timer()
    {
        return ex.timer();
    }

    // A complete header with its payload withheld puts a frame in progress, arming the deadline.
    void feed_header(std::span<const std::byte> whole)
    {
        stream.feed(whole.subspan(0, header_size));
    }

    void feed_payload(std::span<const std::byte> whole)
    {
        stream.feed(whole.subspan(header_size));
    }
};

constexpr std::size_t k_payload = 256;

}

TEST_CASE("wire stream_inbound timer race: a stale expired-timer completion re-armed in the same "
          "turn no-ops the no-progress close",
          "[wire][stream_inbound][race]")
{
    auto whole = encode_complete(k_payload);
    std::span<const std::byte> all{whole};

    race_fixture f;

    f.feed_header(all);
    REQUIRE(f.c.frames == 0);

    // The deadline's expiry is queued, then the frame completes in the same turn, ahead of it.
    f.timer().manufacture_expiry();
    f.feed_payload(all);
    REQUIRE(f.c.frames == 1);

    f.ex.run_posted();
    REQUIRE(f.c.closes == 0);
}

TEST_CASE("wire stream_inbound timer race: a stale expiry queued behind an equal-deadline re-arm "
          "no-ops the no-progress close",
          "[wire][stream_inbound][race]")
{
    auto whole = encode_complete(k_payload);
    std::vector<std::byte> rest(whole.begin() + static_cast<std::ptrdiff_t>(header_size), whole.end());
    rest.insert(rest.end(), whole.begin(), whole.begin() + static_cast<std::ptrdiff_t>(header_size));

    race_fixture f;

    f.feed_header(whole);
    f.timer().manufacture_expiry();
    // One read completes the frame and starts the next one at the same deadline, so the queued expiry
    // finds a frame in progress again.
    f.stream.feed(rest);
    REQUIRE(f.c.frames == 1);

    f.ex.run_posted();
    REQUIRE(f.c.closes == 0);
}

TEST_CASE("wire stream_inbound timer race: a stale expired-timer completion dropped on teardown "
          "runs no callback",
          "[wire][stream_inbound][race]")
{
    auto whole = encode_complete(k_payload);
    std::span<const std::byte> all{whole};

    counters observed;
    {
        race_fixture f;
        f.stream.on_protocol_close([&observed](close_cause) { ++observed.closes; });

        f.feed_header(all);
        f.timer().manufacture_expiry();
        // The executor is torn down with the expiry still queued and drops it uninvoked, as an
        // io_context's destructor does.
    }

    REQUIRE(observed.closes == 0);
}

TEST_CASE("wire stream_inbound timer race: a genuinely elapsed no-progress deadline still closes "
          "exactly once",
          "[wire][stream_inbound][race]")
{
    auto whole = encode_complete(k_payload);
    std::span<const std::byte> all{whole};

    race_fixture f;

    f.feed_header(all);
    REQUIRE(f.c.closes == 0);

    // The armed wait elapses with no cancel and no re-arm: the real timeout fires.
    f.timer().manufacture_expiry();
    f.ex.run_posted();
    REQUIRE(f.c.closes == 1);
    REQUIRE(f.c.last_cause == close_cause::no_progress_timeout);

    f.ex.run_posted();
    REQUIRE(f.c.closes == 1);
}
