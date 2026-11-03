#include "test_subscribe_retry_common.h"

#include "plexus/io/subscriber_qos.h"

#include "plexus/io/detail/drop_event.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/topic_hash.h"

#include <catch2/catch_test_macros.hpp>

#include <array>
#include <chrono>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <algorithm>

using namespace subscribe_retry_fixture;
using namespace std::chrono_literals;
using plexus::io::detail::drop_cause;
using plexus::wire::msg_type;
using received = std::vector<std::string>;

namespace {

plexus::io::subscriber_qos latest()
{
    plexus::io::subscriber_qos q;
    q.durability_mode = plexus::io::durability::latest;
    return q;
}

}

TEST_CASE("subscribe retry: a subscribe that overtakes the handshake response is re-sent and takes effect", "[integration][subscribe_retry]")
{
    retry_link link;
    link.enable_retry(*link.responder);
    link.demand(k_topic);
    link.to_requester.rule = on(msg_type::handshake_resp, relay_action::hold);
    link.start();
    link.release(link.to_requester);
    REQUIRE(link.requester->is_complete());
    REQUIRE(link.req_drops.size() == 1);
    CHECK(link.req_drops[0].cause == drop_cause::unestablished);
    CHECK(link.resp_ready == 0);

    link.advance(250ms);
    CHECK(link.to_responder.count(msg_type::subscribe_response) == 1);
    CHECK(link.resp_ready == 1);
    link.publish(k_topic, "live");
    CHECK(link.resp_received == received{"live"});
}

TEST_CASE("subscribe retry: a session without the retry never re-sends", "[integration][subscribe_retry]")
{
    retry_link link;
    link.demand(k_topic);
    link.to_requester.rule = on(msg_type::subscribe, relay_action::drop, {1});
    link.start();
    link.advance(10s);
    CHECK(link.to_requester.count(msg_type::subscribe) == 1);
    CHECK(link.resp_ready == 0);
}

TEST_CASE("subscribe retry: a dropped subscribe is re-sent and takes effect", "[integration][subscribe_retry]")
{
    retry_link link;
    link.enable_retry(*link.responder);
    link.demand(k_topic);
    link.to_requester.rule = on(msg_type::subscribe, relay_action::drop, {1});
    link.start();
    CHECK(link.resp_ready == 0);

    link.advance(250ms);
    CHECK(link.to_requester.count(msg_type::subscribe) == 2);
    CHECK(link.resp_ready == 1);
    link.publish(k_topic, "live");
    CHECK(link.resp_received == received{"live"});
}

TEST_CASE("subscribe retry: an unanswered subscribe is re-sent four times, then counted unacknowledged and the session is kept", "[integration][subscribe_retry]")
{
    retry_link link;
    link.enable_retry(*link.responder);
    link.demand(k_topic);
    link.to_requester.rule = on(msg_type::subscribe, relay_action::drop);
    link.start();
    const std::array waits{250ms, 500ms, 1000ms, 2000ms};
    for(std::size_t k = 0; k < waits.size(); ++k)
    {
        link.advance(waits[k] - 1ms);
        CHECK(link.to_requester.count(msg_type::subscribe) == k + 1);
        link.advance(1ms);
        CHECK(link.to_requester.count(msg_type::subscribe) == k + 2);
    }
    CHECK(link.resp_ready == 0);

    link.advance(2000ms);
    CHECK(link.to_requester.count(msg_type::subscribe) == 5);
    REQUIRE(link.resp_drops.size() == 1);
    CHECK(link.resp_drops[0].cause == drop_cause::unacknowledged);
    CHECK(link.resp_drops[0].topic_hash == plexus::wire::fqn_topic_hash(k_topic));
    CHECK(link.sink.count("subscribe_unacknowledged") == 1);
    CHECK(link.resp_ready == 1);
    CHECK(link.responder->is_complete());
    link.responder->emit_heartbeat();
    link.ex.drain();
    CHECK(link.req_stamps == 1);
    link.advance(10s);
    CHECK(link.to_requester.count(msg_type::subscribe) == 5);
}

TEST_CASE("subscribe retry: a lost response is re-acknowledged without a second attachment", "[integration][subscribe_retry]")
{
    retry_link link;
    link.enable_retry(*link.responder);
    link.demand(k_latched, latest());
    link.to_responder.rule = on(msg_type::subscribe_response, relay_action::hold, {1});
    link.start();
    CHECK(link.resp_received == received{"latched"});

    link.advance(250ms);
    CHECK(link.to_requester.count(msg_type::subscribe) == 2);
    CHECK(link.to_responder.count(msg_type::subscribe_response) == 2);
    CHECK(link.resp_ready == 1);
    link.release(link.to_responder);
    CHECK(link.resp_ready == 1);
    CHECK(link.sink.lines.empty());
    link.publish(k_latched, "live");
    CHECK(link.resp_received == received{"latched", "live"});
    link.advance(10s);
    CHECK(link.to_requester.count(msg_type::subscribe) == 2);
}

TEST_CASE("subscribe retry: teardown while a retry is armed sends nothing more", "[integration][subscribe_retry]")
{
    retry_link link;
    link.enable_retry(*link.responder);
    link.demand(k_topic);
    link.to_requester.rule = on(msg_type::subscribe, relay_action::drop);
    link.start();
    link.responder->tear_down();
    link.ex.drain();
    const std::uint32_t written = link.to_requester.total();

    link.advance(10s);
    CHECK(link.to_requester.total() == written);
    CHECK(link.responder->pending().settled());
}

TEST_CASE("subscribe retry: a subscribe added while a retry is armed joins it", "[integration][subscribe_retry]")
{
    retry_link link;
    link.enable_retry(*link.responder);
    link.demand(k_topic);
    link.to_requester.rule = on(msg_type::subscribe, relay_action::drop, {1, 2, 4});
    link.start();
    link.advance(100ms);
    link.responder->subscribe(k_other);
    link.ex.drain();
    link.advance(149ms);
    CHECK(link.to_requester.count(msg_type::subscribe) == 2);
    link.advance(1ms);
    CHECK(link.to_requester.count(msg_type::subscribe) == 4);
    CHECK(link.resp_ready == 0);
    link.publish(k_topic, "first");

    link.advance(500ms);
    CHECK(link.to_requester.count(msg_type::subscribe) == 5);
    CHECK(link.resp_ready == 1);
    link.publish(k_other, "second");
    CHECK(link.resp_received == received{"first", "second"});
    link.advance(10s);
    CHECK(link.to_requester.count(msg_type::subscribe) == 5);
}

TEST_CASE("subscribe retry: a declaration dropped before the peer completed is re-sent once", "[integration][subscribe_retry]")
{
    retry_link link;
    link.enable_retry(*link.responder);
    link.resp_messages.declare(k_declared, plexus::topic_qos{});
    link.to_requester.rule = on(msg_type::handshake_resp, relay_action::hold);
    link.start();
    CHECK(link.to_requester.count(msg_type::declare) == 1);
    CHECK(link.req_declared.empty());

    link.release(link.to_requester);
    CHECK(std::ranges::count(link.req_declared, k_declared) == 1);
    link.requester->emit_heartbeat();
    link.advance(10s);
    CHECK(link.to_requester.count(msg_type::declare) == 2);
}
