#include "plexus/io/peer_session.h"
#include "plexus/io/peer_context.h"
#include "plexus/io/message_forwarder.h"
#include "plexus/io/procedure_forwarder.h"
#include "plexus/io/handshake_fsm.h"

#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_transport.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_bus.h"

#include "plexus/policy.h"

#include "plexus/wire/frame_codec.h"
#include "plexus/wire/frame.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <memory>
#include <chrono>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>
#include <string_view>
#include <system_error>

using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_channel;
using plexus::inproc::inproc_transport;
using plexus::inproc::inproc_policy;
using plexus::io::handshake_fsm_config;
using plexus::wire::rpc_status;
using session = plexus::io::peer_session<inproc_policy>;
using msg_forwarder = plexus::io::message_forwarder<inproc_policy>;
using rpc_forwarder = plexus::io::procedure_forwarder<inproc_policy>;

namespace {

constexpr auto k_long_timeout = std::chrono::hours(1);

std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

std::string to_string(std::span<const std::byte> b)
{
    return std::string{reinterpret_cast<const char *>(b.data()), b.size()};
}

handshake_fsm_config make_cfg(std::uint8_t id_seed)
{
    plexus::node_id id{};
    id[0] = std::byte{id_seed};
    return handshake_fsm_config{.self_id = id, .version_major = 1, .version_minor = 0,
                                .compatible_version_major = 1, .compatible_version_minor = 0};
}

// A two-node inproc link stood up through the transport's listen/dial rendezvous
// (no hand-dial): the dialer end becomes the requester (is_inbound_bootstrap=false)
// and the accepted end becomes the responder (is_inbound_bootstrap=true) — the
// realistic asymmetric handshake the B1 bridge completes on both sides. Each node
// owns its channel + forwarders + a peer_session for the single peer it talks to;
// the requester's request drives the responder to complete + answer, and the
// requester completes off that response. The channels are deferred in unique_ptr
// and the sessions in std::optional, built only once dial/on_accepted deliver the
// ends — and declared AFTER the bus/executor/transport so destruction unwinds the
// channels before the bus they registered on. Real messages and RPC ride the SAME
// live channels.
struct link
{
    inproc_bus<> bus;
    inproc_executor<> ex{bus};
    inproc_transport<> transport{ex, bus};

    msg_forwarder req_messages;
    msg_forwarder resp_messages;
    rpc_forwarder req_procedures{ex, k_long_timeout};
    rpc_forwarder resp_procedures{ex, k_long_timeout};

    plexus::io::peer_context<inproc_policy> req_ctx;   // the dialer slot's per-peer record
    plexus::io::peer_context<inproc_policy> resp_ctx;  // the accepted slot's per-peer record
    std::optional<session> requester;
    std::optional<session> responder;

    std::vector<std::string> req_received;
    std::vector<std::string> resp_received;

    explicit link(std::chrono::nanoseconds timeout = k_long_timeout)
    {
        transport.on_accepted([this, timeout](std::unique_ptr<inproc_channel<>> ch) {
            resp_ctx.channel = std::move(ch);
            resp_ctx.node_name = "requester-node";
            responder.emplace(resp_ctx, ex, make_cfg(0x01), timeout,
                              resp_messages, resp_procedures, true);
            responder->on_message([this](std::string_view, std::span<const std::byte> d) {
                resp_received.emplace_back(to_string(d));
            });
            responder->start();
        });
        transport.on_dialed([this, timeout](std::unique_ptr<inproc_channel<>> ch, const plexus::io::endpoint &) {
            req_ctx.channel = std::move(ch);
            req_ctx.node_name = "responder-node";
            requester.emplace(req_ctx, ex, make_cfg(0x02), timeout,
                              req_messages, req_procedures, false);
            requester->on_message([this](std::string_view, std::span<const std::byte> d) {
                req_received.emplace_back(to_string(d));
            });
            requester->start();
        });

        transport.listen({"inproc", "svc"});
        transport.dial({"inproc", "svc"});
    }

    void drive() { ex.drain(); }
};

}

TEST_CASE("inproc peer_session pair completes the handshake, mints epochs, installs once, looped",
          "[integration][peer_session][inproc]")
{
    constexpr int k_iterations = 100;
    int completed = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        link l;
        l.drive();

        REQUIRE(l.requester->is_complete());
        REQUIRE(l.responder->is_complete());
        REQUIRE(l.requester->session_id() != 0);
        REQUIRE(l.responder->session_id() != 0);

        // A further drive does not re-mint or re-install (install-once latch).
        const auto req_epoch = l.requester->session_id();
        l.drive();
        REQUIRE(l.requester->session_id() == req_epoch);
        ++completed;
    }
    REQUIRE(completed == k_iterations);
}

TEST_CASE("inproc peer_session: a dialed (one-directional) connection completes BOTH sides — the accepted bootstrap responder answers, the dialer mints off the response, and gated data flows both ways, looped",
          "[integration][peer_session][inproc]")
{
    constexpr int k_iterations = 100;
    const std::string downward = "dialer-to-responder";
    const std::string upward = "responder-to-dialer";
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        link l;   // the dial rendezvous: only the dialer dials, the accepted end bootstraps
        l.drive();

        // Both complete WITHOUT a simultaneous connect: the accepted bootstrap responder
        // sent an accept response, so the dialer completed and minted its OWN epoch.
        // (Before the response-on-bootstrap-complete fix the dialer stranded and neither
        // flowed.)
        REQUIRE(l.requester->is_complete());
        REQUIRE(l.responder->is_complete());
        REQUIRE(l.requester->session_id() != 0);
        REQUIRE(l.responder->session_id() != 0);

        // The established session is usable BOTH ways, each direction gated by the
        // sender's epoch (the receiver latches it).
        REQUIRE(l.resp_messages.attach(l.responder->msg_peer(), "down"));
        REQUIRE(l.req_messages.attach_for_fanout(l.requester->msg_peer(), "down"));
        REQUIRE(l.req_messages.attach(l.requester->msg_peer(), "up"));
        REQUIRE(l.resp_messages.attach_for_fanout(l.responder->msg_peer(), "up"));
        l.drive();

        l.req_messages.publish("down", as_bytes(downward), l.requester->session_id());
        l.resp_messages.publish("up", as_bytes(upward), l.responder->session_id());
        l.drive();

        REQUIRE(l.resp_received.size() == 1);
        REQUIRE(l.resp_received[0] == downward);
        REQUIRE(l.req_received.size() == 1);
        REQUIRE(l.req_received[0] == upward);
        REQUIRE(l.responder->peer_session_id() == l.requester->session_id());
        REQUIRE(l.requester->peer_session_id() == l.responder->session_id());
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

TEST_CASE("inproc peer_session: a real published message flows post-handshake and latches the epoch, looped",
          "[integration][peer_session][inproc]")
{
    constexpr int k_iterations = 100;
    const std::string payload = "real-published-bytes";
    int delivered = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        link l;
        l.drive();

        // The responder subscribes (so its forwarder resolves the topic_hash on the
        // receive tail); the requester fans the topic toward its peer, then publishes
        // carrying its minted epoch. The frame rides the live channel to the responder.
        REQUIRE(l.resp_messages.attach(l.responder->msg_peer(), "topic"));
        REQUIRE(l.req_messages.attach_for_fanout(l.requester->msg_peer(), "topic"));
        l.drive();
        l.req_messages.publish("topic", as_bytes(payload), l.requester->session_id());
        l.drive();

        REQUIRE(l.resp_received.size() == 1);
        REQUIRE(l.resp_received[0] == payload);
        REQUIRE(l.responder->peer_session_id() == l.requester->session_id());
        ++delivered;
    }
    REQUIRE(delivered == k_iterations);
}

TEST_CASE("inproc peer_session: a real RPC round-trips post-handshake matched by correlation, looped",
          "[integration][peer_session][inproc]")
{
    constexpr int k_iterations = 100;
    const std::string param = "rpc-param";
    int answered = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        link l;
        l.drive();

        l.resp_procedures.serve("svc", [](std::span<const std::byte> p, rpc_forwarder::reply_fn &reply) {
            reply(rpc_status::success, p);   // echo the param back
        });

        int fired = 0;
        rpc_status got = rpc_status::error;
        std::string ret;
        l.req_procedures.call(l.requester->rpc_peer(), "svc", as_bytes(param),
            [&](rpc_status s, std::span<const std::byte> r) { ++fired; got = s; ret = to_string(r); },
            std::nullopt, l.requester->session_id());
        l.drive();

        REQUIRE(fired == 1);
        REQUIRE(got == rpc_status::success);
        REQUIRE(ret == param);
        ++answered;
    }
    REQUIRE(answered == k_iterations);
}

namespace {

// Synthesize a unidirectional data frame for "topic" carrying a chosen session_id,
// exactly as the forwarder frames one — so handing it to a receiver's on_receive
// exercises the production staleness gate, not a hand-strip.
std::vector<std::byte> make_data_frame(const std::string &payload, std::uint8_t session_id)
{
    msg_forwarder framer;
    inproc_bus<> bus;
    inproc_executor<> ex(bus);
    inproc_channel<> capture(ex);
    inproc_channel<> tx(ex);
    tx.connect_to(capture.local_endpoint());
    std::vector<std::byte> captured;
    capture.on_data([&](std::span<const std::byte> f) { captured.assign(f.begin(), f.end()); });
    msg_forwarder::peer peer{tx, "x"};
    framer.attach_for_fanout(peer, "topic");
    ex.drain();
    framer.publish("topic", as_bytes(payload), session_id);
    ex.drain();
    return captured;
}

}

TEST_CASE("inproc peer_session: the data-path staleness gate FIRES — a mismatched epoch is dropped, the latched epoch delivered, looped",
          "[integration][peer_session][inproc]")
{
    constexpr int k_iterations = 100;
    const std::string good = "latched-epoch-bytes";
    const std::string stale = "stale-session-bytes";
    int proven = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        link l;
        l.drive();
        REQUIRE(l.resp_messages.attach(l.responder->msg_peer(), "topic"));
        REQUIRE(l.req_messages.attach_for_fanout(l.requester->msg_peer(), "topic"));
        l.drive();

        // A real publish latches the requester's epoch on the responder.
        l.req_messages.publish("topic", as_bytes(good), l.requester->session_id());
        l.drive();
        REQUIRE(l.resp_received.size() == 1);
        const auto latched = l.responder->peer_session_id();
        REQUIRE(latched == l.requester->session_id());

        // A frame for the SAME topic carrying a DIFFERENT non-zero epoch is dropped:
        // the sink does not grow.
        const std::uint8_t stale_epoch = static_cast<std::uint8_t>(latched == 200 ? 199 : 200);
        auto stale_frame = make_data_frame(stale, stale_epoch);
        l.responder->on_receive(stale_frame);
        l.drive();
        REQUIRE(l.resp_received.size() == 1);   // DROPPED, not delivered

        // A frame carrying the latched epoch IS delivered.
        auto fresh_frame = make_data_frame(good, latched);
        l.responder->on_receive(fresh_frame);
        l.drive();
        REQUIRE(l.resp_received.size() == 2);
        REQUIRE(l.resp_received[1] == good);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}

namespace {

struct manual_clock
{
    using duration = std::chrono::nanoseconds;
    using rep = duration::rep;
    using period = duration::period;
    using time_point = std::chrono::time_point<manual_clock>;
    static constexpr bool is_steady = false;

    static inline time_point current{};
    static time_point now() noexcept { return current; }
    static void reset() noexcept { current = time_point{}; }
    static void advance(duration d) noexcept { current += d; }
};

struct manual_policy
{
    using executor_type = inproc_executor<manual_clock> &;
    using byte_channel_type = inproc_channel<manual_clock>;
    using timer_type = plexus::inproc::inproc_timer<manual_clock>;
    using byte_owner = std::shared_ptr<const void>;

    static void post(executor_type ex, plexus::detail::move_only_function<void()> fn) { ex.post(std::move(fn)); }
};

static_assert(plexus::Policy<manual_policy>);

using manual_session = plexus::io::peer_session<manual_policy>;
using manual_msg = plexus::io::message_forwarder<manual_policy>;
using manual_rpc = plexus::io::procedure_forwarder<manual_policy>;

// A lone requester on the manual clock with no responder ever answering: the
// handshake never completes, so the armed handshake timer is what resolves it.
struct timeout_harness
{
    inproc_bus<manual_clock> bus;
    inproc_executor<manual_clock> ex{bus};
    inproc_channel<manual_clock> peer_ch{ex};   // a silent peer that never responds

    manual_msg messages;
    manual_rpc procedures{ex, std::chrono::hours(1)};

    plexus::io::peer_context<manual_policy> ctx;   // the record owns the dialer channel
    manual_session requester;

    explicit timeout_harness(std::chrono::nanoseconds timeout)
        : ctx{std::make_unique<inproc_channel<manual_clock>>(ex), {}, "silent-node", {}, {}}
        , requester(ctx, ex, make_cfg(0x02), timeout, messages, procedures, false)
    {
        ctx.channel->connect_to(peer_ch.local_endpoint());   // sends land on a peer that never replies
        requester.start();
    }

    void drive() { ex.drain(); }
};

}

TEST_CASE("inproc peer_session: a handshake that never completes aborts once the timeout passes (virtual clock), looped",
          "[integration][peer_session][inproc]")
{
    for(int iter = 0; iter < 100; ++iter)
    {
        manual_clock::reset();
        const auto deadline = std::chrono::milliseconds(50);
        timeout_harness h(deadline);

        // Before the deadline: the request was sent but no response arrived — no abort.
        h.drive();
        REQUIRE(!h.requester.is_complete());

        // Cross the deadline and drive: on_timeout fires abort -> tear_down.
        manual_clock::advance(deadline + std::chrono::milliseconds(1));
        h.drive();
        REQUIRE(!h.requester.is_complete());
        REQUIRE(h.requester.peer_session_id() == 0);   // epoch latch reset by teardown
    }
}

namespace {

// A manual-clock pair stood up through the manual-clock transport rendezvous (no
// hand-dial) so the handshake CAN complete before the deadline; used to prove the
// timer is cancelled on completion (no later abort fires).
struct manual_link
{
    inproc_bus<manual_clock> bus;
    inproc_executor<manual_clock> ex{bus};
    inproc_transport<manual_clock> transport{ex, bus};

    manual_msg req_messages;
    manual_msg resp_messages;
    manual_rpc req_procedures{ex, std::chrono::hours(1)};
    manual_rpc resp_procedures{ex, std::chrono::hours(1)};

    plexus::io::peer_context<manual_policy> req_ctx;   // the dialer slot's per-peer record
    plexus::io::peer_context<manual_policy> resp_ctx;  // the accepted slot's per-peer record
    std::optional<manual_session> requester;
    std::optional<manual_session> responder;

    explicit manual_link(std::chrono::nanoseconds timeout)
    {
        transport.on_accepted([this, timeout](std::unique_ptr<inproc_channel<manual_clock>> ch) {
            resp_ctx.channel = std::move(ch);
            resp_ctx.node_name = "requester-node";
            responder.emplace(resp_ctx, ex, make_cfg(0x01), timeout,
                              resp_messages, resp_procedures, true);
            responder->start();
        });
        transport.on_dialed([this, timeout](std::unique_ptr<inproc_channel<manual_clock>> ch, const plexus::io::endpoint &) {
            req_ctx.channel = std::move(ch);
            req_ctx.node_name = "responder-node";
            requester.emplace(req_ctx, ex, make_cfg(0x02), timeout,
                              req_messages, req_procedures, false);
            requester->start();
        });
        transport.listen({"inproc", "svc"});
        transport.dial({"inproc", "svc"});
    }

    void drive() { ex.drain(); }
};

}

TEST_CASE("inproc peer_session: completing before the deadline cancels the timer — no later abort, looped",
          "[integration][peer_session][inproc]")
{
    for(int iter = 0; iter < 100; ++iter)
    {
        manual_clock::reset();
        const auto deadline = std::chrono::milliseconds(50);
        manual_link l(deadline);

        l.drive();
        REQUIRE(l.requester->is_complete());
        REQUIRE(l.responder->is_complete());

        // Advance well past the (now-cancelled) deadline: NO abort fires; the
        // session stays complete with its minted epoch intact.
        const auto epoch = l.requester->session_id();
        manual_clock::advance(deadline + std::chrono::seconds(10));
        l.drive();
        REQUIRE(l.requester->is_complete());
        REQUIRE(l.requester->session_id() == epoch);
    }
}

TEST_CASE("inproc peer_session: teardown drains the forwarders and resets the epoch latch",
          "[integration][peer_session][inproc]")
{
    link l;
    l.drive();
    REQUIRE(l.req_messages.attach_for_fanout(l.requester->msg_peer(), "topic"));
    l.drive();

    // Latch the responder's view of the requester's epoch with a real publish.
    l.req_messages.publish("topic", as_bytes(std::string{"x"}), l.requester->session_id());
    l.drive();
    REQUIRE(l.responder->peer_session_id() == l.requester->session_id());

    l.responder->tear_down();
    REQUIRE(!l.responder->is_complete());
    REQUIRE(l.responder->peer_session_id() == 0);

    // After teardown the responder no longer fans toward its peer: a publish from
    // the responder reaches nobody (detach_all dropped the fan-out entry).
    l.resp_messages.attach_for_fanout(l.responder->msg_peer(), "back");
    l.resp_messages.detach_all(l.responder->msg_peer());
    l.resp_messages.publish("back", as_bytes(std::string{"y"}), l.responder->session_id());
    l.drive();
    REQUIRE(l.req_received.empty());
}
