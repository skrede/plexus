#include "test_stream_inbound_common.h"

#include "plexus/detail/compat.h"

#include <vector>
#include <utility>
#include <cstddef>
#include <system_error>

using namespace stream_inbound_fixture;

namespace {

// The shared inproc_timer completes its handler synchronously, so it cannot model an
// already-expired asio completion: one that is queued with ec==success and can no longer be
// recalled by cancel(). This mock separates the currently-armed wait (m_pending) from expiries
// that have already been posted (m_posted); a test can manufacture an expiry, drive a same-turn
// cancel/re-arm, and only then run the stale completion. The posted completions are owned by the
// timer, so tearing the object down drops them without invoking them.
struct race_executor
{
    void register_timer(struct race_timer *t) noexcept
    {
        timer = t;
    }

    struct race_timer *timer{nullptr};
};

struct race_timer
{
    using handler_t = plexus::detail::move_only_function<void(std::error_code)>;

    explicit race_timer(race_executor &ex)
    {
        ex.register_timer(this);
    }

    void expires_after(std::chrono::milliseconds)
    {
        cancel_pending();
    }

    void async_wait(handler_t h)
    {
        m_pending = std::move(h);
    }

    void cancel()
    {
        cancel_pending();
    }

    void manufacture_expiry()
    {
        if(m_pending)
            m_posted.push_back(std::exchange(m_pending, handler_t{}));
    }

    void run_posted()
    {
        for(auto &h : m_posted)
            if(h)
                h(std::error_code{});
        m_posted.clear();
    }

    void fire_pending()
    {
        if(m_pending)
        {
            auto h = std::exchange(m_pending, handler_t{});
            h(std::error_code{});
        }
    }

private:
    void cancel_pending()
    {
        if(m_pending)
        {
            auto h = std::exchange(m_pending, handler_t{});
            h(std::make_error_code(std::errc::operation_canceled));
        }
    }

    handler_t m_pending;
    std::vector<handler_t> m_posted;
};

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
        return *ex.timer;
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

    // Post the deadline's expiry, then complete the frame in the same turn: the completing feed
    // cancels the timer and advances the generation past the stale, already-posted completion.
    f.timer().manufacture_expiry();
    f.feed_payload(all);
    REQUIRE(f.c.frames == 1);

    f.timer().run_posted();
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
        f.stream.on_protocol_close([&observed](close_cause cause) { ++observed.closes; (void)cause; });

        f.feed_header(all);
        f.timer().manufacture_expiry();
        // The fixture (and its member timer) goes out of scope here with the expiry still posted;
        // a timer that owns its completions drops them without invoking the captured handler.
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

    // The armed wait genuinely elapses (no cancel, no re-arm): the live generation matches, so the
    // real timeout fires.
    f.timer().fire_pending();
    REQUIRE(f.c.closes == 1);
    REQUIRE(f.c.last_cause == close_cause::no_progress_timeout);

    // A second run finds nothing posted; the close is not duplicated.
    f.timer().run_posted();
    REQUIRE(f.c.closes == 1);
}
