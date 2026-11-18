#include "test_authenticated_channel_common.h"

#include "plexus/crypto/datagram_authenticated_channel.h"

#include "plexus/io/polymorphic_byte_channel.h"

#include "plexus/io/detail/scheduler_key.h"
#include "plexus/io/detail/egress_channel_traits.h"

#include <memory>
#include <cstdint>
#include <utility>

using namespace authenticated_channel_fixture;

namespace {

class keyed_lower : public wire_lower
{
public:
    keyed_lower()
            : m_key(plexus::io::detail::next_scheduler_key())
    {
    }

    std::uint64_t scheduler_key() const
    {
        return m_key;
    }

private:
    std::uint64_t m_key;
};

template<typename Wrapper>
void check_forwards_lower_key()
{
    keyed_lower lower;
    auto wrapper = std::make_unique<Wrapper>(lower, aead_cipher_id::chacha20_poly1305, fixed_keys());
    CHECK(plexus::io::detail::key_of(*wrapper) == lower.scheduler_key());

    plexus::io::polymorphic_byte_channel erased{std::make_unique<plexus::io::channel_adapter<Wrapper>>(std::move(wrapper))};
    CHECK(erased.scheduler_key() == lower.scheduler_key());
}

}

TEST_CASE("crypto.authenticated_channel keys the egress bands by its lower channel's per-construction key", "[crypto][authenticated_channel]")
{
    check_forwards_lower_key<authenticated_channel<keyed_lower>>();
}

TEST_CASE("crypto.datagram_authenticated_channel keys the egress bands by its lower channel's per-construction key", "[crypto][authenticated_channel]")
{
    check_forwards_lower_key<plexus::crypto::datagram_authenticated_channel<keyed_lower>>();
}
