#include "plexus/io/detail/warn_line.h"

#include "plexus/log/logger.h"

#include "support/alloc_counter.h"

#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <algorithm>
#include <string_view>

using plexus::io::detail::warn_line;
using plexus::io::detail::rate_limited_warning;
using std::chrono::steady_clock;

namespace {

constexpr std::string_view k_line = "plexus: test event";
constexpr std::chrono::seconds k_interval{1};
constexpr std::chrono::milliseconds k_tick{10};
const steady_clock::time_point k_start{std::chrono::hours(1)};

struct tick_clock
{
    using duration   = std::chrono::nanoseconds;
    using rep        = duration::rep;
    using period     = duration::period;
    using time_point = std::chrono::time_point<tick_clock>;

    static time_point now() noexcept
    {
        return time_point{};
    }
};

struct recording_logger final : plexus::log::logger
{
    void warn(std::string_view message) override
    {
        lines.emplace_back(message);
    }

    std::vector<std::string> lines;
};

struct counting_logger final : plexus::log::logger
{
    void warn(std::string_view message) override
    {
        ++lines;
        longest = std::max(longest, message.size());
    }

    std::uint32_t lines{0};
    std::size_t longest{0};
};

}

TEST_CASE("warn line: the first note warns at once and the first after the interval carries the suppressed count", "[io][warn_line]")
{
    recording_logger logger;
    rate_limited_warning<steady_clock> warning(k_interval);

    warning.note(logger, k_line, k_start);
    REQUIRE(logger.lines == std::vector<std::string>{std::string(k_line)});
    for(std::uint32_t i = 1; i <= 41; ++i)
        warning.note(logger, k_line, k_start + i * k_tick);
    CHECK(logger.lines.size() == 1);

    warning.note(logger, k_line, k_start + k_interval);
    REQUIRE(logger.lines.size() == 2);
    CHECK(logger.lines.back() == "plexus: test event, 41 more");
}

TEST_CASE("warn line: a note after a quiet interval warns the plain line", "[io][warn_line]")
{
    recording_logger logger;
    rate_limited_warning<steady_clock> warning(k_interval);

    warning.note(logger, k_line, k_start);
    warning.note(logger, k_line, k_start + 2 * k_interval);

    CHECK(logger.lines == std::vector<std::string>{std::string(k_line), std::string(k_line)});
}

TEST_CASE("warn line: an append past the end truncates at the buffer", "[io][warn_line]")
{
    warn_line line;
    line.append(std::string(warn_line::k_capacity - 2, 'x'));
    line.append_count(1234);
    CHECK(line.view().size() == warn_line::k_capacity - 2);

    line.append(", more text");
    CHECK(line.view().size() == warn_line::k_capacity);
    CHECK(line.view().substr(warn_line::k_capacity - 2) == ", ");
}

TEST_CASE("warn line: a summary never exceeds the buffer and is built without allocating", "[io][warn_line]")
{
    const std::string long_line(3 * warn_line::k_capacity, 'x');
    counting_logger logger;
    rate_limited_warning<steady_clock> warning(k_interval);
    warning.note(logger, long_line, k_start);

    plexus::testing::reset_alloc_count();
    const std::size_t before = plexus::testing::alloc_count();
    for(std::uint32_t i = 1; i <= 100; ++i)
    {
        warning.note(logger, k_line, k_start + i * k_interval - k_tick);
        warning.note(logger, long_line, k_start + i * k_interval);
    }
    const std::size_t after = plexus::testing::alloc_count();

    CHECK(logger.lines == 101);
    CHECK(logger.longest == warn_line::k_capacity);
    CHECK(after - before == 0);
}

TEST_CASE("warn line: the interval is measured on the limiter's own clock", "[io][warn_line]")
{
    recording_logger logger;
    rate_limited_warning<tick_clock> warning(k_interval);
    const tick_clock::time_point t0 = tick_clock::now();

    warning.note(logger, k_line, t0);
    warning.note(logger, k_line, t0 + k_interval - tick_clock::duration{1});
    REQUIRE(logger.lines.size() == 1);

    warning.note(logger, k_line, t0 + k_interval);
    CHECK(logger.lines == std::vector<std::string>{std::string(k_line), "plexus: test event, 1 more"});
}
