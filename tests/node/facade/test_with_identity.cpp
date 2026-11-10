#include "test_self_loopback_common.h"

#include "plexus/expected.h"
#include "plexus/publisher.h"
#include "plexus/subscriber.h"
#include "plexus/wire_bytes.h"
#include "plexus/typed_codec.h"
#include "plexus/with_identity.h"
#include "plexus/typed_publisher_options.h"

#include "plexus/graph/topic_record.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <memory>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <type_traits>
#include <system_error>

namespace {

using plexus_test::sample;
using plexus_test::fixture;
using plexus::graph::topic_role;

struct le32_codec
{
    using value_type = sample;

    plexus::wire_bytes<> encode(const sample &v) const
    {
        auto owner = std::make_shared<std::array<std::byte, 4>>();
        for(std::size_t i = 0; i < owner->size(); ++i)
            (*owner)[i] = static_cast<std::byte>((v.value >> (8 * i)) & 0xffu);
        const std::span<const std::byte> view{*owner};
        return plexus::wire_bytes<>{view, std::move(owner)};
    }

    plexus::expected<void, std::error_code> decode(std::span<const std::byte> bytes, sample &out) const
    {
        if(bytes.size() != 4)
            return plexus::expected<void, std::error_code>{plexus::unexpect, std::make_error_code(std::errc::invalid_argument)};
        out.value = 0;
        for(std::size_t i = 0; i < bytes.size(); ++i)
            out.value |= static_cast<std::uint32_t>(bytes[i]) << (8 * i);
        return {};
    }
};

using adapted = plexus::with_identity<le32_codec>;

constexpr plexus::type_identity k_identity{0x5EED0001u, "le32_sample", 7u};

static_assert(!plexus::identity_bearing<le32_codec>);
static_assert(plexus::typed_codec<adapted>);
static_assert(plexus::identity_bearing<adapted>);
static_assert(!std::is_default_constructible_v<adapted>);

std::vector<std::string> declared_types(fixture &f, topic_role role)
{
    std::array<plexus::graph::topic_record, 8> records{};
    const auto swept = f.node().topics(records);
    std::vector<std::string> names;
    for(std::size_t i = 0; i < swept.count; ++i)
        if(records[i].role == role)
            for(std::size_t n = 0; n < records[i].types.count; ++n)
                names.emplace_back(records[i].types.names[n]);
    return names;
}

}

TEST_CASE("with_identity: type_info returns exactly the identity it was built with", "[node][typed]")
{
    const adapted codec{le32_codec{}, k_identity};

    REQUIRE(codec.type_info().type_id == k_identity.type_id);
    REQUIRE(codec.type_info().type_name == k_identity.type_name);
    REQUIRE(codec.type_info().schema_hint == k_identity.schema_hint);
}

TEST_CASE("with_identity: encode and decode round-trip through the adapted codec", "[node][typed]")
{
    adapted codec{le32_codec{}, k_identity};

    const auto bytes = codec.encode(sample{0xA1B2C3D4u});
    sample decoded{};

    REQUIRE(bytes.size() == 4);
    REQUIRE(codec.decode(bytes, decoded).has_value());
    REQUIRE(decoded.value == 0xA1B2C3D4u);
}

TEST_CASE("with_identity: endpoints over the adapter deliver in-process and declare its identity", "[node][typed]")
{
    fixture f;
    std::vector<std::uint32_t> seen;
    plexus::subscriber<adapted> sub{f.node(), "topic", plexus::typed_subscriber_options{}, [&](const sample &v) { seen.push_back(v.value); }, adapted{le32_codec{}, k_identity}};
    auto pub = f.node().advertise("topic", plexus::typed_publisher_options{}, adapted{le32_codec{}, k_identity});
    f.drive();

    pub.publish(sample{0x1234u});
    f.drive();

    REQUIRE(seen == std::vector<std::uint32_t>{0x1234u});
    REQUIRE(declared_types(f, topic_role::publisher) == std::vector<std::string>{"le32_sample"});
    REQUIRE(declared_types(f, topic_role::subscriber) == std::vector<std::string>{"le32_sample"});
}

TEST_CASE("with_identity: an options type_id still overrides the adapter's identity", "[node][typed]")
{
    fixture f;
    plexus::typed_publisher_options opts{};
    opts.type_id = plexus::type_identity{0x0FF5E7u, "override"};

    auto pub = f.node().advertise("topic", opts, adapted{le32_codec{}, k_identity});
    f.drive();

    REQUIRE(declared_types(f, topic_role::publisher) == std::vector<std::string>{"override"});
}
