#include "plexus/detail/driver_record.h"

#include <catch2/catch_test_macros.hpp>

#include <cstdint>

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
