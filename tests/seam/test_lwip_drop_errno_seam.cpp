// The loopback peer cannot be made to lose a connection on demand, so these cases force the errno lwIP reports for one
// onto the host socket's send or receive.

#include "lwip_rx_rig.h"

#include "plexus/freertos/freertos_executor.h"

#include "plexus/freertos/detail/lwip_errors.h"

#include "plexus/io/io_error.h"

#include <catch2/catch_test_macros.hpp>
#include <catch2/generators/catch_generators.hpp>

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <memory>
#include <thread>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <initializer_list>

using plexus::test::rx_rig;
using plexus::test::rx_channel;

namespace {

constexpr std::array<std::byte, 4> k_payload{std::byte{0xDE}, std::byte{0xAD}, std::byte{0xBE}, std::byte{0xEF}};

class forced_errno
{
public:
    explicit forced_errno(int e)
    {
        plexus::test::host_forced_errno.store(e);
    }

    forced_errno(const forced_errno &)            = delete;
    forced_errno &operator=(const forced_errno &) = delete;

    ~forced_errno()
    {
        plexus::test::host_forced_errno.store(0);
    }
};

struct failure_count
{
    std::int32_t errors = 0;
    std::int32_t closes = 0;
    std::optional<plexus::io::io_error> last;
    std::thread::id error_thread;
};

void count_failures(rx_channel &channel, failure_count &seen)
{
    channel.on_error(
            [&seen](plexus::io::io_error e)
            {
                ++seen.errors;
                seen.last         = e;
                seen.error_thread = std::this_thread::get_id();
            });
    channel.on_closed([&seen] { ++seen.closes; });
}

std::unique_ptr<rx_channel> polled_channel(plexus::test::host_tcp_socket socket, plexus::freertos::freertos_executor &ex)
{
    return std::make_unique<rx_channel>(std::move(socket), ex, plexus::io::endpoint{"tcp", "127.0.0.1:0"});
}

bool finishes(const std::atomic<bool> &done)
{
    return plexus::test::poll_until([] {}, [&done] { return done.load(); }, std::chrono::seconds{1});
}

}

TEST_CASE("lwip drop: a lost connection the receive task sees fails the channel on the executor", "[seam][lwip_drop]")
{
    const int e = GENERATE(ECONNABORTED, ENOTCONN, ETIMEDOUT);
    CAPTURE(e);
    rx_rig rig;
    failure_count seen;
    count_failures(*rig.channel, seen);
    REQUIRE(rig.spawn() == pdTRUE);

    std::atomic<bool> done{false};
    bool finished = false;
    {
        const forced_errno forced{e};
        std::thread task(
                [&done]
                {
                    plexus::test::run_spawned_task();
                    done = true;
                });
        finished = finishes(done);
        if(!finished)
            rig.channel->close();
        task.join();
    }
    const std::int32_t errors_before_drain = seen.errors;
    rig.ex.drain();

    CHECK(finished);
    CHECK(errors_before_drain == 0);
    CHECK(seen.errors == 1);
    CHECK(seen.error_thread == std::this_thread::get_id());
    CHECK(seen.closes == 1);
}

TEST_CASE("lwip drop: a lost connection the cooperative poll sees fails the channel", "[seam][lwip_drop]")
{
    const int e = GENERATE(ECONNABORTED, ENOTCONN, ETIMEDOUT);
    CAPTURE(e);
    auto pair = plexus::test::make_loopback_pair();
    REQUIRE(pair.has_value());
    plexus::freertos::freertos_executor ex;
    auto channel = polled_channel(std::move(pair->accepted), ex);
    failure_count seen;
    count_failures(*channel, seen);
    {
        const forced_errno forced{e};
        channel->poll();
    }
    ex.drain();

    CHECK(seen.errors == 1);
    CHECK(seen.last == plexus::io::io_error::connection_reset);
    CHECK(channel->closed());
    channel->close();
    ex.drain();
}

TEST_CASE("lwip drop: a send on a lost connection fails the channel instead of stalling", "[seam][lwip_drop]")
{
    const int e = GENERATE(ENOTCONN, ECONNABORTED);
    CAPTURE(e);
    auto pair = plexus::test::make_loopback_pair();
    REQUIRE(pair.has_value());
    plexus::freertos::freertos_executor ex;
    auto channel = polled_channel(std::move(pair->dialed), ex);
    failure_count seen;
    count_failures(*channel, seen);
    {
        const forced_errno forced{e};
        channel->send(plexus::test::on_wire(k_payload));
    }
    ex.drain();

    CHECK(seen.errors == 1);
    CHECK(seen.last == plexus::io::io_error::connection_reset);
    channel->close();
    ex.drain();
}

TEST_CASE("lwip drop: a transient errno leaves the channel open", "[seam][lwip_drop]")
{
    const int e = GENERATE(EWOULDBLOCK, EAGAIN, ENOMEM, EINTR);
    CAPTURE(e);
    auto pair = plexus::test::make_loopback_pair();
    REQUIRE(pair.has_value());
    plexus::freertos::freertos_executor ex;
    auto channel = polled_channel(std::move(pair->accepted), ex);
    failure_count seen;
    count_failures(*channel, seen);
    {
        const forced_errno forced{e};
        channel->poll();
    }
    ex.drain();

    CHECK(seen.errors == 0);
    CHECK_FALSE(channel->closed());
    channel->close();
    ex.drain();
}

TEST_CASE("lwip drop: only the transient errnos keep a socket open", "[seam][lwip_drop]")
{
    for(const int e : {EWOULDBLOCK, EAGAIN, ENOMEM, EINTR})
        CHECK(plexus::freertos::detail::transient_socket_errno(e));
    for(const int e : {ECONNABORTED, ENOTCONN, ETIMEDOUT, ECONNRESET, EPIPE, EHOSTUNREACH, EBADF})
        CHECK_FALSE(plexus::freertos::detail::transient_socket_errno(e));
}
