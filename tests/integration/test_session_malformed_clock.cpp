#include "test_session_malformed_common.h"

#include "plexus/io/peer_session.h"

#include "plexus/io/detail/warn_line.h"
#include "plexus/io/detail/drop_event.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <vector>
#include <cstdint>
#include <algorithm>
#include <type_traits>

using namespace session_malformed_fixture;
using plexus::io::detail::drop_cause;
using plexus::io::detail::k_malformed_warning_interval;

static_assert(std::is_same_v<plexus::io::peer_session<routing_inproc_fixture::manual_policy>::clock_type, routing_inproc_fixture::manual_clock>);

namespace {

void send_undecodable(engine_with_raw_peer &link, std::uint32_t count)
{
    for(std::uint32_t i = 0; i < count; ++i)
    {
        link.channel->send(undecodable_subscribe());
        link.ex.drain();
    }
}

std::vector<std::string> scripted_run()
{
    engine_clock::reset();
    engine_with_raw_peer link;
    REQUIRE(link.channel);
    link.channel->send(handshake_naming(0x02));
    link.ex.drain();

    send_undecodable(link, 3);
    engine_clock::advance(engine_clock::duration{k_malformed_warning_interval} / 2);
    send_undecodable(link, 2);
    engine_clock::advance(k_malformed_warning_interval);
    send_undecodable(link, 1);
    return link.logger.malformed_lines();
}

}

TEST_CASE("session malformed: the warning interval follows the session's clock", "[integration][session_malformed]")
{
    engine_clock::reset();
    drop_recorder recorder;
    engine_with_raw_peer link;
    link.node.add_observer(recorder);
    REQUIRE(link.channel);
    link.channel->send(handshake_naming(0x02));
    link.ex.drain();

    send_undecodable(link, 1000);
    CHECK(link.logger.malformed_lines().size() == 1);

    engine_clock::advance(k_malformed_warning_interval);
    send_undecodable(link, 1);

    CHECK(link.logger.malformed_lines() == std::vector<std::string>{std::string(k_malformed_line), "plexus: peer_session malformed_frame, 999 more"});
    CHECK(recorder.seen.size() == 1001);
    CHECK(std::ranges::count(recorder.seen, drop_cause::malformed) == 1001);
}

TEST_CASE("session malformed: two identical runs on the session's clock log the same warnings", "[integration][session_malformed]")
{
    const std::vector<std::string> first  = scripted_run();
    const std::vector<std::string> second = scripted_run();

    CHECK(first == second);
    CHECK(first == std::vector<std::string>{std::string(k_malformed_line), "plexus: peer_session malformed_frame, 4 more"});
}
