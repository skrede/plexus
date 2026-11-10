// Must not compile: its ctest passes only on the identity diagnostic.

#include "test_self_loopback_common.h"

#include "plexus/expected.h"
#include "plexus/wire_bytes.h"
#include "plexus/value_logger.h"
#include "plexus/value_logger_options.h"

#include <span>
#include <cstddef>
#include <cstdint>
#include <ostream>
#include <sstream>
#include <system_error>

namespace {

struct reading
{
    std::uint32_t value{};
};

std::ostream &operator<<(std::ostream &out, const reading &r)
{
    return out << r.value;
}

struct anonymous_codec
{
    using value_type = reading;

    plexus::wire_bytes<> encode(const reading &) const
    {
        return {};
    }

    plexus::expected<void, std::error_code> decode(std::span<const std::byte>, reading &) const
    {
        return {};
    }
};

}

int main()
{
    plexus_test::fixture f;
    std::ostringstream out;
    auto logger = f.node().log<anonymous_codec>("topic", plexus::value_logger_options{.out = out});
    f.drive();
}
