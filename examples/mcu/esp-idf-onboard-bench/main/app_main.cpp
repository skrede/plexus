// The on-device onboard intra-node zero-copy bench firmware: a single node whose SOLE transport is
// the intra-node self-route. The device publishes a typed request to its OWN "request" subscriber;
// that subscriber echoes the payload to "reply"; the "reply" subscriber stops the round-trip clock
// and issues the next request. All on-chip with ZERO link — no UART, no Wi-Fi, no lwIP, no second
// node, no wire. The typed lane delivers each message BY ADDRESS, so the codec's encode is never
// invoked on the path; the firmware shares one encode counter across every endpoint and emits it as
// the zero-copy witness alongside the per-tier latency samples. This is the on-device counterpart to
// the host intra-node loopback number.

#include "onboard_policy.h"
#include "bench_workload.h"

#include "plexus/publisher.h"
#include "plexus/subscriber.h"
#include "plexus/loopback_node.h"
#include "plexus/node_options.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/io/message_info.h"
#include "plexus/io/reconnect_config.h"

#include "plexus/freertos/device_runtime.h"
#include "plexus/freertos/freertos_executor.h"

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"

#include "esp_timer.h"
#include "esp_heap_caps.h"

#include <span>
#include <memory>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <system_error>

namespace {

constexpr const char *k_policy = "intra-onboard";

// Sized with headroom over the engine's self-route footprint plus the 4 KiB loan-pool message and
// the largest tier buffer. ESP32 services peripheral ISRs on the active task's stack, so the budget
// absorbs worst-case interrupt nesting on top of the deepest plexus path.
constexpr std::uint32_t k_plexus_task_stack = 16384; // bytes (xTaskCreate takes bytes)
constexpr UBaseType_t   k_plexus_task_prio  = 5;

using codec_t = example::message_codec;

// The reply leg: the "request" subscriber receives the request message BY ADDRESS on the typed
// lane, then republishes the same bytes on "reply" through a borrowed loan (zero-copy out as well).
struct echo_leg
{
    plexus::publisher<codec_t> &reply;

    void on_request(const example::message &req)
    {
        auto loan = reply.borrow();
        if(!loan)
            return;
        loan->length = req.length;
        loan->seq    = req.seq;
        const std::size_t n = req.length <= example::k_max_tier_bytes ? req.length : example::k_max_tier_bytes;
        for(std::size_t i = 0; i < n; ++i)
            loan->buffer[i] = req.buffer[i];
        reply.publish(std::move(loan));
    }
};

// The cell sweep: it issues one tier-sized request at a time, stamping the send time and a sequence
// tag, and on the matching reply records the round trip then issues the next. Once every tier drains
// it emits the zero-copy witness (the shared encode count) and the free-heap resource line, then
// stops issuing.
struct bench_runner
{
    plexus::publisher<codec_t>        &request;
    std::shared_ptr<std::atomic<int>>  encodes;
    std::size_t                        tier_index{0};
    std::uint32_t                      count{0};
    std::uint8_t                       seq{0};
    std::int64_t                       sent_us{0};
    bool                               awaiting{false};
    bool                               done{false};

    void issue()
    {
        const std::size_t tier = example::k_payload_tiers[tier_index];
        auto loan = request.borrow();
        if(!loan)
            return;
        loan->length = static_cast<std::uint32_t>(tier);
        loan->seq    = ++seq;
        loan->buffer[0] = static_cast<std::byte>(seq);
        sent_us  = esp_timer_get_time();
        awaiting = true;
        request.publish(std::move(loan));
    }

    void on_reply(const example::message &echo)
    {
        if(done || !awaiting || echo.seq != seq)
            return;
        const std::int64_t rtt = esp_timer_get_time() - sent_us;
        awaiting = false;
        record(rtt);
        advance();
    }

    void record(std::int64_t rtt_us)
    {
        const std::size_t tier = example::k_payload_tiers[tier_index];
        if(count >= example::k_warmup_requests)
            example::emit_sample(k_policy, tier, count - example::k_warmup_requests, rtt_us);
        ++count;
    }

    void advance()
    {
        if(count < example::k_warmup_requests + example::k_sampled_requests)
            return issue();
        count = 0;
        if(++tier_index < example::k_payload_tiers.size())
            return issue();
        done = true;
        example::emit_witness(k_policy, encodes->load());
        example::emit_resource(k_policy, esp_get_free_heap_size());
    }
};

// The no-op poll handle the borrow-only device facade drives. The onboard node owns no transport to
// poll, but the facade's run loop carries the watchdog-safe park discipline (drain, then yield the
// core for a bounded interval); reusing it with an inert poll keeps that discipline without
// hand-rolling a super-loop.
struct idle_poll
{
    void poll() noexcept
    {
    }
};

// The user's one task: it owns the executor and the loopback host, then hands the executor to the
// device facade to drive. The engine is heap-allocated so it does not sit on this task's stack;
// ex/disc are task-scope locals declared ABOVE the host — the host borrows them by reference, so
// they must outlive it. One shared codec instance backs all four endpoints, so its single encode
// counter witnesses every serialization anywhere on the round-trip path.
void plexus_task(void *)
{
    using namespace std::chrono_literals;

    plexus::freertos::freertos_executor ex;
    plexus::discovery::static_discovery disc{{}};

    plexus::node_options opts;
    opts.name              = "esp32-onboard-bench";
    opts.max_message_bytes = example::k_max_tier_bytes;
    opts.reconnect         = plexus::io::reconnect_config{200ms, 5s, std::nullopt, std::nullopt};
    opts.redial_seed       = 0x0B0AB0DEu;

    auto  host = std::make_unique<plexus::loopback_host<example::onboard_policy>>(ex, disc, plexus::node_id_from_name("esp32-onboard-bench"), opts);
    auto &node = host->node();

    codec_t codec;
    auto    encodes = codec.encodes;

    plexus::publisher<codec_t> reply_pub{node, "reply", plexus::typed_publisher_options{}, codec};
    echo_leg echo{reply_pub};
    plexus::subscriber<codec_t> request_sub{
        node, "request", plexus::typed_subscriber_options{},
        [&](const example::message &req, const plexus::io::message_info &) { echo.on_request(req); },
        codec};

    plexus::publisher<codec_t> request_pub{node, "request", plexus::typed_publisher_options{}, codec};
    bench_runner runner{request_pub, encodes};
    plexus::subscriber<codec_t> reply_sub{
        node, "reply", plexus::typed_subscriber_options{},
        [&](const example::message &echo_msg, const plexus::io::message_info &) { runner.on_reply(echo_msg); },
        codec};

    runner.issue();

    idle_poll idle;
    plexus::freertos::run(ex, idle);
}

}

extern "C" void app_main()
{
    xTaskCreate(plexus_task, "plexus", k_plexus_task_stack, nullptr, k_plexus_task_prio, nullptr);
}
