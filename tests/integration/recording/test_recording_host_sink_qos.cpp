#include "test_recording_host_sink_common.h"

using namespace host_sink_fixture;

TEST_CASE("recording_host_sink record-rate every_nth decimation drops before the ring", "[recording_host_sink][record_rate]")
{
    in_memory_byte_sink mem;
    flat_recorder rec{mem, 64u * 1024u, [n = std::uint64_t{0}]() mutable { return ++n; }};
    rec.open(make_node(4), topic_capture_rule{});

    recording_sink tap{rec};
    record_rate_rule rule;
    rule.every_nth = 3;
    tap.set_record_rates({{plexus::wire::fqn_topic_hash("t/dec"), rule}});

    for(int i = 0; i < 9; ++i)
    {
        message_info info{};
        info.publication_sequence = static_cast<std::uint64_t>(i);
        const std::string body = "d" + std::to_string(i);
        tap.on_message_delivered("t/dec", info, message_view{bytes_of(body), {}});
    }
    while(rec.pump())
        ;
    rec.flush();

    REQUIRE(count_samples(mem.bytes(), plexus::wire::fqn_topic_hash("t/dec")) == 3);
}

TEST_CASE("recording_host_sink record-rate disabled topic records nothing", "[recording_host_sink][record_rate]")
{
    in_memory_byte_sink mem;
    flat_recorder rec{mem, 64u * 1024u, [n = std::uint64_t{0}]() mutable { return ++n; }};
    rec.open(make_node(4), topic_capture_rule{});

    recording_sink tap{rec};
    record_rate_rule rule;
    rule.enabled = false;
    tap.set_record_rates({{plexus::wire::fqn_topic_hash("t/off"), rule}});

    for(int i = 0; i < 5; ++i)
    {
        message_info info{};
        info.publication_sequence = static_cast<std::uint64_t>(i);
        tap.on_message_delivered("t/off", info, message_view{bytes_of(std::string{"z"}), {}});
    }
    while(rec.pump())
        ;
    rec.flush();

    REQUIRE(count_samples(mem.bytes(), plexus::wire::fqn_topic_hash("t/off")) == 0);
}

TEST_CASE("recording_host_sink record-rate max_hz throttles by the observer clock", "[recording_host_sink][record_rate]")
{
    in_memory_byte_sink mem;
    flat_recorder rec{mem, 64u * 1024u, [n = std::uint64_t{0}]() mutable { return ++n; }};
    rec.open(make_node(4), topic_capture_rule{});

    std::uint64_t now = 0;
    recording_sink tap{rec};
    tap.set_clock([&now] { return now; });
    record_rate_rule rule;
    rule.max_hz = 1000.0; // min gap 1e6 ns
    tap.set_record_rates({{plexus::wire::fqn_topic_hash("t/hz"), rule}});

    const std::array<std::uint64_t, 5> times{0u, 500'000u, 1'000'000u, 1'400'000u, 2'000'000u};
    for(std::uint64_t t : times)
    {
        now = t;
        message_info info{};
        tap.on_message_delivered("t/hz", info, message_view{bytes_of(std::string{"y"}), {}});
    }
    while(rec.pump())
        ;
    rec.flush();

    REQUIRE(count_samples(mem.bytes(), plexus::wire::fqn_topic_hash("t/hz")) == 3);
}
