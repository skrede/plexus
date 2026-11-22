#include "plexus/freertos/lwip_channel.h"
#include "plexus/freertos/freertos_executor.h"
#include "plexus/freertos/lwip_channel_limits.h"

#include "plexus/freertos/detail/null_stream_socket.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/congestion.h"

#include <catch2/catch_test_macros.hpp>

#include <utility>
#include <stdexcept>

namespace {

using null_socket  = plexus::freertos::detail::null_stream_socket;
using lwip_channel = plexus::freertos::lwip_channel<null_socket>;
using lim          = plexus::freertos::lwip_channel_limits;

lwip_channel make_channel(plexus::freertos::freertos_executor &ex, plexus::io::congestion c)
{
    plexus::io::endpoint remote{"tcp", "127.0.0.1:9000"};
    return lwip_channel{null_socket{}, ex, std::move(remote), lim::read_buffer_bytes, lim::max_message_bytes, lim::reassembly_bytes, lim::egress_cap_bytes, c};
}

}

TEST_CASE("lwip congestion: an lwIP channel configured to drop its oldest frames is refused when built", "[seam]")
{
    plexus::freertos::freertos_executor ex;
    CHECK_THROWS_AS(make_channel(ex, plexus::io::congestion::drop_oldest), std::runtime_error);
    CHECK_NOTHROW(make_channel(ex, plexus::io::congestion::drop_newest));
    CHECK_NOTHROW(make_channel(ex, plexus::io::congestion::block));
}
