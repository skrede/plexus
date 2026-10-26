#include "plexus/detail/driver_record.h"

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <optional>

namespace {

using record_type = plexus::detail::driver_record<std::uint32_t>;

constexpr std::uint32_t k_driver = 1;
constexpr std::uint32_t k_next   = 2;

}

TEST_CASE("driver_record release returns a claimed record to unbound", "[io][driver_record]")
{
    record_type record;
    {
        const record_type::step_scope scope(record, k_driver);
        REQUIRE(record.running_in(k_driver));
    }
    CHECK_FALSE(record.running_in(k_next));
    record.release();
    CHECK(record.running_in(k_next));
    CHECK_FALSE(record.running_in(k_driver));
}

TEST_CASE("driver_record a nested step on the same thread keeps it inside until the outermost step ends", "[io][driver_record]")
{
    record_type record;
    std::optional<record_type::step_scope> outer;
    outer.emplace(record, k_driver);
    {
        const record_type::step_scope nested(record, k_driver);
        CHECK(record.inside(k_driver));
    }
    CHECK(record.inside(k_driver));
    outer.reset();
    CHECK_FALSE(record.inside(k_driver));
}

TEST_CASE("driver_record overlapping steps leave no stale record when the first stepper leaves first", "[io][driver_record]")
{
    record_type record;
    std::optional<record_type::step_scope> first;
    std::optional<record_type::step_scope> second;
    first.emplace(record, k_driver);
    second.emplace(record, k_next);
    first.reset();
    second.reset();
    CHECK_FALSE(record.inside(k_driver));
    CHECK_FALSE(record.inside(k_next));
}

TEST_CASE("driver_record overlapping steps leave no stale record when the second stepper leaves first", "[io][driver_record]")
{
    record_type record;
    std::optional<record_type::step_scope> first;
    std::optional<record_type::step_scope> second;
    first.emplace(record, k_driver);
    second.emplace(record, k_next);
    second.reset();
    first.reset();
    CHECK_FALSE(record.inside(k_driver));
    CHECK_FALSE(record.inside(k_next));
}
