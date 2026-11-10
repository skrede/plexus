// Must not compile: its ctest passes only on the identity diagnostic.

#include "test_self_loopback_common.h"

#include "plexus/expected.h"
#include "plexus/subscriber.h"
#include "plexus/wire_bytes.h"

#include <span>
#include <cstddef>
#include <system_error>

namespace {

struct anonymous_codec
{
    using value_type = plexus_test::sample;

    plexus::wire_bytes<> encode(const plexus_test::sample &) const
    {
        return {};
    }

    plexus::expected<void, std::error_code> decode(std::span<const std::byte>, plexus_test::sample &) const
    {
        return {};
    }
};

}

int main()
{
    plexus_test::fixture f;
    plexus::subscriber<anonymous_codec> sub{f.node(), "topic", [](const plexus_test::sample &) {}};
    f.drive();
}
