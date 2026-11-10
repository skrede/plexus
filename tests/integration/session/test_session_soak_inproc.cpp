#include "test_session_soak_common.h"

#include "plexus/io/node_name.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <string>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <charconv>
#include <string_view>
#include <system_error>

using namespace session_soak_fixture;

namespace {

constexpr std::int32_t k_default_cycles = 1000;
constexpr std::int32_t k_sample_every   = 500;

std::int32_t soak_cycles()
{
    const char *text = std::getenv("PLEXUS_SOAK_CYCLES");
    if(text == nullptr)
        return k_default_cycles;

    const std::string_view digits{text};
    std::int32_t n       = 0;
    const auto [end, ec] = std::from_chars(digits.data(), digits.data() + digits.size(), n);
    return ec == std::errc{} && end == digits.data() + digits.size() && n > 0 ? n : k_default_cycles;
}

using witnesses = std::array<std::size_t, 11>;

constexpr std::array<std::string_view, 11> k_witness_names{"slots_a",  "slots_b",  "retiring_a", "retiring_b", "known_a",  "known_b",
                                                           "topics_a", "topics_b", "demands",    "origins_a",  "origins_b"};

template<typename Engine>
std::size_t known_rows(const Engine &e)
{
    std::size_t n = 0;
    e.known().for_each_candidate([&n](const auto &...) { ++n; });
    return n;
}

template<typename Engine>
std::size_t topic_records(const Engine &e)
{
    std::size_t n = 0;
    e.topic_table().for_each([&n](const auto &) { ++n; });
    return n;
}

witnesses sample(soak_net &net)
{
    const std::size_t demands = net.b.messages().remembered_topics(plexus::io::node_name_of(net.id_a)).size();
    return witnesses{net.a.registry().slot_count(),
                     net.b.registry().slot_count(),
                     net.transport_a.retiring_size(),
                     net.transport_b.retiring_size(),
                     known_rows(net.a),
                     known_rows(net.b),
                     topic_records(net.a),
                     topic_records(net.b),
                     demands,
                     net.a.reported_origin_count(),
                     net.b.reported_origin_count()};
}

std::string describe(const witnesses &w)
{
    std::string line;
    for(std::size_t i = 0; i < w.size(); ++i)
        line += " " + std::string{k_witness_names[i]} + "=" + std::to_string(w[i]);
    return line;
}

void require_at_baseline(const witnesses &now, const witnesses &base)
{
    for(std::size_t i = 0; i < now.size(); ++i)
    {
        INFO(k_witness_names[i]);
        REQUIRE(now[i] == base[i]);
    }
}

// Returns b's slot count while the session is held, its highest point in the cycle.
std::size_t run_cycle(soak_net &net, bool from_listener, std::size_t idle_b)
{
    net.connect();
    net.hold();
    const std::size_t held = net.b.registry().slot_count();
    if(from_listener)
        net.accepted_from_a()->tear_down();
    else
        net.a.registry().hang_up(net.id_b);
    net.ex.drain();

    // A full-jitter redial can complete in the same step as the teardown, so b may already hold its next session's slot.
    REQUIRE(net.step_until([&net, idle_b] { return net.b.registry().slot_count() == idle_b + (net.b.is_connected(net.id_a) ? 1 : 0); }));
    return held;
}

}

// The last cycle ends from the dialer's side, as the baseline cycle does, so neither count is taken with a redial pending.
TEST_CASE("session soak: connect and disconnect cycles return slot, channel and enumeration counts to baseline", "[integration][soak]")
{
    manual_clock::reset();
    const std::int32_t cycles = soak_cycles();
    soak_net net;
    const std::size_t idle_b = net.b.registry().slot_count();

    run_cycle(net, false, idle_b);
    const witnesses base = sample(net);

    for(std::int32_t cycle = 1; cycle <= cycles; ++cycle)
    {
        const std::size_t held = run_cycle(net, (cycles - cycle) % 2 == 1, idle_b);
        if(cycle % k_sample_every == 0)
            CHECK(held <= idle_b + 2);
    }

    const witnesses after = sample(net);
    require_at_baseline(after, base);
    REQUIRE(net.seen.silenced == 0);
    REQUIRE(net.seen.connected == cycles + 1);
    REQUIRE(net.seen.disconnected == cycles + 1);
    WARN("cycles=" << cycles << describe(after));
}
