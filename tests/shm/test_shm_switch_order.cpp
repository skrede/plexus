#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/demand_transition.h"
#include "plexus/io/message_forwarder.h"
#include "plexus/io/detail/egress_scheduler.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/data_frame.h"
#include "plexus/wire/close_cause.h"
#include "plexus/wire/frame_codec.h"

#include "plexus/log/logger.h"

#include "plexus/policy.h"
#include "plexus/topic_qos.h"
#include "plexus/detail/compat.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <chrono>
#include <memory>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <string_view>
#include <system_error>

namespace {

using arrival_log = std::vector<std::string>;

// A data frame's body, "subscribe" for a subscribe request, and "" for every other control frame.
std::string arrival_of(std::span<const std::byte> frame)
{
    const auto header = plexus::wire::decode_header(frame);
    if(header && header->type == plexus::wire::msg_type::subscribe)
        return "subscribe";
    if(!header || header->type != plexus::wire::msg_type::unidirectional)
        return {};
    const auto data = plexus::wire::decode_unidirectional(frame.subspan(plexus::wire::header_size));
    return data ? std::string{reinterpret_cast<const char *>(data->data.data()), data->data.size()} : std::string{};
}

// A banded channel whose backpressure the test controls; it appends every frame it is handed to a log both channels
// share, so the log reads in the order the subscriber would see the frames.
struct order_channel
{
    order_channel(arrival_log &log, std::size_t &occupancy, std::uint64_t key)
            : m_key(key)
            , m_log(log)
            , m_occupancy(occupancy)
    {
    }

    void send(std::span<const std::byte> frame)
    {
        if(std::string arrival = arrival_of(frame); !arrival.empty())
            m_log.get().push_back(std::move(arrival));
    }

    void close()
    {
    }

    plexus::io::endpoint remote_endpoint() const
    {
        return {"tcp", "127.0.0.1:0"};
    }

    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)>)
    {
    }

    void on_closed(plexus::detail::move_only_function<void()>)
    {
    }

    void on_error(plexus::detail::move_only_function<void(plexus::io::io_error)>)
    {
    }

    void on_protocol_close(plexus::detail::move_only_function<void(plexus::wire::close_cause)>)
    {
    }

    std::size_t backpressured() const noexcept
    {
        return m_occupancy.get();
    }

    std::uint64_t scheduler_key() const noexcept
    {
        return m_key;
    }

private:
    std::uint64_t m_key;
    std::reference_wrapper<arrival_log> m_log;
    std::reference_wrapper<std::size_t> m_occupancy;
};

struct order_executor
{
};

struct order_timer
{
    explicit order_timer(order_executor &)
    {
    }

    order_timer(order_executor &, std::error_code &)
    {
    }

    void expires_after(std::chrono::milliseconds)
    {
    }

    void async_wait(plexus::detail::move_only_function<void(std::error_code)>)
    {
    }

    void cancel()
    {
    }
};

struct order_policy
{
    using executor_type     = order_executor &;
    using byte_channel_type = order_channel;
    using timer_type        = order_timer;
    using byte_owner        = std::shared_ptr<const void>;

    static void post(executor_type, plexus::detail::move_only_function<void()> fn)
    {
        fn();
    }
};

static_assert(plexus::Policy<order_policy>);

using order_forwarder = plexus::io::message_forwarder<order_policy>;

std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

}

TEST_CASE("switch_order a late companion route never overtakes frames queued on the wire", "[shm][switch_order][!shouldfail]")
{
    constexpr std::uint32_t k_switch_at  = 4;
    constexpr std::uint32_t k_unstall_at = 8;
    constexpr std::uint32_t k_publishes  = 12;
    arrival_log log;
    std::size_t wire_occupancy = plexus::io::detail::k_low_water + 1;
    std::size_t ring_occupancy = 0;
    order_channel wire{log, wire_occupancy, 1};
    order_channel ring{log, ring_occupancy, 2};
    plexus::log::null_logger sink;
    order_forwarder fwd{sink};
    fwd.declare("t", plexus::topic_qos{});
    REQUIRE(fwd.attach_for_fanout(order_forwarder::peer{wire, "node-a"}, "t"));

    std::uint32_t n = 0;
    fwd.on_companion_route([&](std::string_view, std::string_view, std::size_t) { return n >= k_switch_at ? &ring : nullptr; });
    arrival_log expected;
    for(n = 1; n <= k_publishes; ++n)
    {
        wire_occupancy = n >= k_unstall_at ? 0 : wire_occupancy;
        fwd.publish("t", as_bytes(std::to_string(n)));
        expected.push_back(std::to_string(n));
    }
    REQUIRE(log == expected);
}

TEST_CASE("attach_order the subscriber mints its receive ring before its subscribe leaves", "[shm][attach_order][!shouldfail]")
{
    arrival_log log;
    std::size_t occupancy = 0;
    order_channel wire{log, occupancy, 3};
    plexus::log::null_logger sink;
    order_forwarder fwd{sink};
    fwd.on_demand_transition(
            [&](std::string_view, std::string_view, plexus::io::demand_transition dir, plexus::io::demand_role role)
            {
                if(dir == plexus::io::demand_transition::up && role == plexus::io::demand_role::subscriber)
                    log.emplace_back("up");
            });

    REQUIRE(fwd.attach(order_forwarder::peer{wire, "node-a"}, "t"));
    REQUIRE(log == arrival_log{"up", "subscribe"});
}
