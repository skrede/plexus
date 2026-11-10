#include "support/posted_queue.h"
#include "support/noop_notifier.h"

#include "plexus/shm/shm_mux_member.h"

#include "plexus/native/shm_region_broker.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"
#include "plexus/io/detail/drop_event.h"

#include "plexus/wire/close_cause.h"

#include "plexus/testing/platform.h"

#include "plexus/detail/compat.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <memory>
#include <string>
#include <cstddef>
#include <cstdint>
#include <utility>

namespace {

using member = plexus::shm::shm_mux_member<plexus::native::shm_region_broker, plexus::testing::noop_notifier>;

struct dialed_channel
{
    explicit dialed_channel(const char *tag)
            : queue()
            , broker()
            , shm(broker, queue.post(), plexus::io::reliability::reliable, plexus::io::congestion::block)
            , channel()
    {
        shm.on_dialed([this](std::unique_ptr<member::channel_type> ch, const plexus::io::endpoint &) { channel = std::move(ch); });
        shm.dial(plexus::io::endpoint{"shm", std::string("byte_channel_retire.") + tag + "." + std::to_string(plexus::testing::process_id())});
    }

    plexus::testing::posted_queue queue;
    plexus::native::shm_region_broker broker;
    member shm;
    std::unique_ptr<member::channel_type> channel;
};

}

TEST_CASE("shm_byte_channel_retire close posts on_closed instead of invoking it inside close()", "[shm][byte_channel_retire]")
{
    dialed_channel d{"posted"};
    REQUIRE(d.channel);
    bool closing        = false;
    bool inside_close   = false;
    std::int32_t closes = 0;
    d.channel->on_closed(
            [&]
            {
                ++closes;
                inside_close = closing;
            });

    closing = true;
    d.channel->close();
    closing = false;
    CHECK(closes == 0);

    d.queue.drain();
    CHECK(closes == 1);
    CHECK_FALSE(inside_close);
}

TEST_CASE("shm_byte_channel_retire a channel closed twice posts one notification", "[shm][byte_channel_retire]")
{
    dialed_channel d{"once"};
    REQUIRE(d.channel);
    std::int32_t closes = 0;
    d.channel->on_closed([&closes] { ++closes; });

    d.channel->close();
    d.channel->close();
    CHECK(d.queue.size() == 1);
    d.queue.drain();
    d.channel->close();
    d.queue.drain();
    CHECK(closes == 1);
}

TEST_CASE("shm_byte_channel_retire a handler installed after the close is posted", "[shm][byte_channel_retire]")
{
    dialed_channel d{"late"};
    REQUIRE(d.channel);
    d.channel->close();
    d.queue.drain();

    std::int32_t closes = 0;
    d.channel->on_closed([&closes] { ++closes; });
    CHECK(closes == 0);
    d.queue.drain();
    CHECK(closes == 1);
}

TEST_CASE("shm_byte_channel_retire a consumer callback is detached once the channel closed", "[shm][byte_channel_retire]")
{
    dialed_channel d{"detach"};
    REQUIRE(d.channel);
    auto held                               = std::make_shared<std::int32_t>(0);
    const std::weak_ptr<std::int32_t> watch = held;
    d.channel->on_data([held](std::span<const std::byte>) {});
    d.channel->on_error([held](plexus::io::io_error) {});
    d.channel->on_drop([held](const plexus::io::detail::drop_event &) {});
    d.channel->on_protocol_close([held](plexus::wire::close_cause) {});
    held.reset();

    d.channel->close();
    CHECK(watch.expired());
}
