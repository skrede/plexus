// Debug turns the single-executor check on, and under it a moved-from call aborts; these cases pin the inert path.
#undef PLEXUS_AFFINITY_CHECKS

#include "plexus/publisher.h"
#include "plexus/subscriber.h"
#include "plexus/value_logger.h"

#include "test_self_loopback_common.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <ostream>
#include <sstream>
#include <utility>

using plexus_test::sample;
using plexus_test::fixture;
using plexus_test::counting_codec;

namespace plexus_test {

std::ostream &operator<<(std::ostream &os, const sample &s)
{
    return os << s.value;
}

}

namespace {

using typed_publisher  = plexus::publisher<counting_codec>;
using typed_subscriber = plexus::subscriber<counting_codec>;
using typed_logger     = plexus::value_logger<counting_codec, plexus::no_projection>;

constexpr std::array<std::byte, 3> k_malformed{std::byte{1}, std::byte{2}, std::byte{3}};

}

TEST_CASE("handles: a moved-from bytes publisher's publish is inert", "[node][handles]")
{
    fixture f;
    std::int32_t got = 0;
    plexus::subscriber<> sub{f.node(), "topic", [&](std::span<const std::byte>) { ++got; }};
    plexus::publisher<> source{f.node(), "topic"};
    plexus::publisher<> target{std::move(source)};
    f.drive();
    source.publish(k_malformed);
    target.publish(k_malformed);
    f.drive();
    REQUIRE(got == 1);
}

TEST_CASE("handles: a moved-from typed publisher's publish is inert", "[node][handles]")
{
    fixture f;
    std::vector<std::uint32_t> got;
    typed_subscriber sub{f.node(), "topic", [&](const sample &v) { got.push_back(v.value); }};
    typed_publisher source{f.node(), "topic"};
    typed_publisher target{std::move(source)};
    f.drive();
    source.publish(sample{7});
    target.publish(sample{9});
    f.drive();
    REQUIRE(got == std::vector<std::uint32_t>{9});
}

TEST_CASE("handles: a moved-from typed publisher's publish of a loan is inert and leaves the loan held", "[node][handles]")
{
    fixture f;
    std::vector<std::uint32_t> got;
    typed_subscriber sub{f.node(), "topic", [&](const sample &v) { got.push_back(v.value); }};
    typed_publisher source{f.node(), "topic"};
    typed_publisher target{std::move(source)};
    f.drive();
    auto held = target.borrow(sample{5});
    source.publish(std::move(held));
    REQUIRE(held);
    target.publish(std::move(held));
    f.drive();
    REQUIRE(got == std::vector<std::uint32_t>{5});
}

TEST_CASE("handles: a moved-from typed publisher's borrow returns an empty loan", "[node][handles]")
{
    fixture f;
    typed_publisher source{f.node(), "topic"};
    typed_publisher target{std::move(source)};
    REQUIRE(!source.borrow());
    REQUIRE(target.borrow());
}

TEST_CASE("handles: a moved-from typed publisher's loan_exhausted is 0", "[node][handles]")
{
    fixture f;
    typed_publisher source{f.node(), "topic", plexus::typed_publisher_options{.pool_depth = 1}};
    {
        auto held = source.borrow();
        source.publish(sample{1});
    }
    typed_publisher target{std::move(source)};
    REQUIRE(source.loan_exhausted() == 0);
    REQUIRE(target.loan_exhausted() == 1);
}

TEST_CASE("handles: a moved-from typed subscriber's decode_failed is 0", "[node][handles]")
{
    fixture f;
    typed_subscriber source{f.node(), "topic", [](const sample &) {}};
    typed_subscriber target{std::move(source)};
    plexus::publisher<> raw{f.node(), "topic"};
    f.drive();
    raw.publish(k_malformed);
    f.drive();
    REQUIRE(source.decode_failed() == 0);
    REQUIRE(target.decode_failed() == 1);
}

TEST_CASE("handles: a moved-from value_logger's decode_failed is 0", "[node][handles]")
{
    fixture f;
    std::ostringstream out;
    typed_logger source{f.node(), "topic", plexus::value_logger_options{.out = out}};
    typed_logger target{std::move(source)};
    plexus::publisher<> raw{f.node(), "topic"};
    f.drive();
    raw.publish(k_malformed);
    f.drive();
    REQUIRE(source.decode_failed() == 0);
    REQUIRE(target.decode_failed() == 1);
}
