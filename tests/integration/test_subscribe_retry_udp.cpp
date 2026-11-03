#include "support/loss_reorder_shim.h"

#include "plexus/node.h"
#include "plexus/node_id.h"
#include "plexus/publisher.h"
#include "plexus/subscriber.h"
#include "plexus/node_options.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/observer.h"
#include "plexus/io/reconnect_config.h"
#include "plexus/io/subscribe_retry_config.h"

#include "plexus/io/detail/drop_event.h"

#include "plexus/asio/udp_policy.h"
#include "plexus/asio/udp_transport.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/testing/pump.h"

#include <asio/io_context.hpp>

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <chrono>
#include <string>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

namespace {

using namespace std::chrono_literals;
using plexus::io::subscribe_retry_config;
using plexus::testing::loss_reorder_config;
using udp_node = plexus::node<plexus::asio::udp_policy, plexus::asio::udp_transport>;

constexpr std::string_view k_to_s = "retry/to_s";
constexpr std::string_view k_to_c = "retry/to_c";
constexpr std::array<std::byte, 1> k_payload{std::byte{0x2A}};
// With this seed the back leg releases s's subscribe to c ahead of s's handshake response.
constexpr loss_reorder_config k_overtake{.reorder_depth = 2, .seed = 5};

struct unestablished_counter final : plexus::io::observer
{
    std::uint32_t drops{0};

    void on_drop(const plexus::io::detail::drop_event &ev) override
    {
        drops += ev.cause == plexus::io::detail::drop_cause::unestablished ? 1u : 0u;
    }
};

plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

plexus::node_options options(std::uint64_t seed, const subscribe_retry_config &retry)
{
    plexus::node_options opts;
    opts.reconnect       = plexus::io::reconnect_config{20ms, 40ms, std::nullopt, std::nullopt};
    opts.redial_seed     = seed;
    opts.subscribe_retry = retry;
    return opts;
}

// Node s listens behind the relay and c dials the relay, so the back schedule shapes everything s sends to c.
class relay_pair
{
public:
    relay_pair(const loss_reorder_config &front, const loss_reorder_config &back, const subscribe_retry_config &retry_at_s = {})
            : m_at_s(0)
            , m_at_c(0)
            , m_io()
            , m_drops()
            , m_ts(m_io)
            , m_tc(m_io)
            , m_disc_s({})
            , m_disc_c({})
            , m_s(m_io, m_disc_s, make_id(0x05), m_ts, options(0x5EEDu, retry_at_s))
            , m_c(m_io, m_disc_c, make_id(0x0C), m_tc, options(0xC0DEu, {}))
            , m_pub_c(m_c, k_to_s)
            , m_pub_s(m_s, k_to_c)
            , m_sub_s(m_s, k_to_s, [this](std::span<const std::byte>) { ++m_at_s; })
            , m_sub_c(m_c, k_to_c, [this](std::span<const std::byte>) { ++m_at_c; })
            , m_relay()
    {
        m_c.router().add_observer(m_drops);
        m_s.listen({"udp", "127.0.0.1:0"});
        plexus::testing::pump_until(m_io, [this] { return m_ts.port() != 0; }, 2s);
        m_relay.emplace(m_io, m_ts.port(), front, back);
        m_c.dial({"udp", "127.0.0.1:" + std::to_string(m_relay->port())});
    }

    template<typename Pred>
    bool pump_for(std::chrono::milliseconds bound, Pred done)
    {
        std::chrono::steady_clock::time_point next = std::chrono::steady_clock::now();
        const auto publish_and_check               = [&]
        {
            if(std::chrono::steady_clock::now() >= next)
            {
                m_pub_c.publish(k_payload);
                m_pub_s.publish(k_payload);
                next += 20ms;
            }
            return done();
        };
        return plexus::testing::pump_until(m_io, publish_and_check, bound);
    }

    std::uint32_t unestablished_drops() const
    {
        return m_drops.drops;
    }

    std::uint32_t delivered_to_s() const
    {
        return m_at_s;
    }

    std::uint32_t delivered_to_c() const
    {
        return m_at_c;
    }

private:
    std::uint32_t m_at_s;
    std::uint32_t m_at_c;
    ::asio::io_context m_io;
    unestablished_counter m_drops;
    plexus::asio::udp_transport m_ts;
    plexus::asio::udp_transport m_tc;
    plexus::discovery::static_discovery m_disc_s;
    plexus::discovery::static_discovery m_disc_c;
    udp_node m_s;
    udp_node m_c;
    plexus::publisher<> m_pub_c;
    plexus::publisher<> m_pub_s;
    plexus::subscriber<> m_sub_s;
    plexus::subscriber<> m_sub_c;
    std::optional<plexus::testing::loss_reorder_relay> m_relay;
};

}

TEST_CASE("subscribe retry udp: a subscribe that overtakes the handshake response still takes effect", "[integration][subscribe_retry]")
{
    relay_pair pair{loss_reorder_config{}, k_overtake};

    CHECK(pair.pump_for(5s, [&] { return pair.delivered_to_s() > 0; }));
    CHECK(pair.unestablished_drops() >= 1);
}

TEST_CASE("subscribe retry udp: with the retry disabled the overtaken subscribe never takes effect", "[integration][subscribe_retry]")
{
    relay_pair pair{loss_reorder_config{}, k_overtake, subscribe_retry_config{.max_attempts = 0}};

    pair.pump_for(5s, [] { return false; });
    CHECK(pair.unestablished_drops() >= 1);
    CHECK(pair.delivered_to_c() > 0);
    CHECK(pair.delivered_to_s() == 0);
}

TEST_CASE("subscribe retry udp: data flows on a leg that drops in both directions", "[integration][subscribe_retry]")
{
    // With these seeds data reaches s only through the re-send.
    relay_pair pair{loss_reorder_config{.loss_num = 20, .seed = 4}, loss_reorder_config{.loss_num = 20, .seed = 104}};

    CHECK(pair.pump_for(5s, [&] { return pair.delivered_to_s() > 0 && pair.delivered_to_c() > 0; }));
}
