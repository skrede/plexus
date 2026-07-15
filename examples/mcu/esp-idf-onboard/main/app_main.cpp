// The onboard intra-node self-delivery example: a single node whose SOLE transport is the
// intra-node self-route. It publishes a typed value to its OWN subscriber on-device — the
// publication is posted onto the cooperative executor and delivered to the subscriber on the
// next drain, all on-chip with ZERO link. There is no UART, no Wi-Fi, no lwIP, no second node
// and no wire: the loopback channel never touches a peripheral. This is the "MCU as a
// self-contained data-plane node" case — onboard plexus compute, the engine fanning a producer
// to a consumer inside one device.
//
// The payload is a plain in-process counter — no HAL, no pin: the point is onboard self-delivery,
// not sensor IO, so no driver dependency is pulled in (the pure-comms invariant stays trivial).
// The node lives off the task stack — heap-allocated once at setup so the constrained main stack
// need not carry the engine. The composition runs in a dedicated task the example creates; the
// user owns that task and drives the executor in it through the device facade, with no background
// plexus thread.

#include "onboard_policy.h"

#include "plexus/publisher.h"
#include "plexus/subscriber.h"
#include "plexus/loopback_node.h"
#include "plexus/node_options.h"

#include "plexus/graph/participant_record.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/io/reconnect_config.h"
#include "plexus/io/message_info.h"

#include "plexus/freertos/device_runtime.h"
#include "plexus/freertos/freertos_timer.h"
#include "plexus/freertos/freertos_executor.h"

#include "plexus/expected.h"
#include "plexus/wire_bytes.h"
#include "plexus/typed_codec.h"

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"

#include "esp_log.h"

#include <span>
#include <array>
#include <memory>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <system_error>

namespace {

constexpr const char *k_tag = "plexus-onboard";

// Sized with headroom over the engine's self-route footprint (no wire stack, no handshake on this
// path). ESP32 services peripheral ISRs on the active task's stack, so the budget absorbs
// worst-case interrupt nesting on top of the deepest plexus path.
constexpr std::uint32_t k_plexus_task_stack = 8192; // bytes (ESP-IDF xTaskCreate takes bytes)
constexpr UBaseType_t k_plexus_task_prio    = 5;

// The onboard payload: a single incrementing counter, the minimal typed value to witness
// self-delivery. No pin, no HAL — a pure in-process datum.
struct reading
{
    std::uint32_t count{};
};

// A trivial little-endian codec for `reading`. On the onboard typed lane encode() is NEVER
// invoked — the self-route delivers the object zero-copy by address — so the heap-backed encode
// body below stays off the hot path; it exists only to satisfy the typed_codec concept and the
// (unused) serialize fallback. decode() is the wire-receive leg, equally unused on the self-route.
struct reading_codec
{
    using value_type = reading;

    plexus::wire_bytes<> encode(const reading &v) const
    {
        auto owner = std::make_shared<std::array<std::byte, 4>>();
        for(int i = 0; i < 4; ++i)
            (*owner)[i] = static_cast<std::byte>((v.count >> (8 * i)) & 0xff);
        std::span<const std::byte> view{owner->data(), owner->size()};
        return plexus::wire_bytes<>{view, std::move(owner)};
    }

    plexus::expected<void, std::error_code> decode(std::span<const std::byte> bytes, reading &out) const
    {
        if(bytes.size() != 4)
            return plexus::expected<void, std::error_code>{plexus::unexpect, std::make_error_code(std::errc::invalid_argument)};
        std::uint32_t v = 0;
        for(int i = 0; i < 4; ++i)
            v |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(bytes[i])) << (8 * i);
        out.count = v;
        return {};
    }

    plexus::type_identity type_info() const
    {
        return {0x0B0A0D70u, "reading"};
    }
};

static_assert(plexus::typed_codec<reading_codec>);

// The periodic publish loop. arm() installs a FRESH handler each cycle that publishes the next
// counter value then re-arms via arm() — a self-rescheduling timer without copying a move-only
// handler (each async_wait move-constructs a new lambda, the move-only-function-safe analog of the
// host steady_timer publish loop).
struct publish_loop
{
    plexus::freertos::freertos_timer &timer;
    plexus::publisher<reading_codec> &topic;
    std::uint32_t next{};

    void arm()
    {
        timer.expires_after(std::chrono::milliseconds{1000});
        timer.async_wait(
            [this](std::error_code)
            {
                topic.publish(reading{next++});
                arm();
            });
    }
};

// The no-op poll handle the borrow-only device facade drives. The onboard node owns no transport
// to poll, but the facade's run loop carries the watchdog-safe park discipline (drain, then yield
// the core for a bounded interval) — reusing it with an inert poll keeps that discipline without
// hand-rolling a super-loop. The timer fires from inside the executor's pump(), so the periodic
// publish and its self-delivery both ride the drain the facade already runs.
struct idle_poll
{
    void poll() noexcept
    {
    }
};

// The user's one task: it owns the executor and the loopback host, then hands the executor to the
// device facade to drive. The engine is heap-allocated here so it does not sit on this task's
// stack; ex/disc are task-scope locals declared ABOVE the host — the host borrows them by
// reference, so they must outlive it. This is the example's own task, not a plexus-spawned thread.
void plexus_task(void *)
{
    using namespace std::chrono_literals;

    plexus::freertos::freertos_executor ex;

    // No peers, no seeds: the only delivery path is the node's own self-route, so the discovery
    // table is empty. The node is discoverable from birth; there is nothing to discover.
    plexus::discovery::static_discovery disc{{}};

    plexus::node_options opts;
    opts.name = "esp32-onboard";
    // Dial the per-message ceiling DOWN for the target — the self-route payload is a 4-byte
    // counter; a PC-sized default would over-provision the loan pools the engine carries.
    opts.max_message_bytes = 64;
    // reconnect / redial_seed are required by node_options even though the self-route never dials
    // or reconnects; set them explicitly so the config is fully specified (fail-closed), never an
    // accidental zero default.
    opts.reconnect   = plexus::io::reconnect_config{200ms, 5s, std::nullopt, std::nullopt};
    opts.redial_seed = 0x0B0AC0DEu;

    auto host = std::make_unique<plexus::loopback_host<example::onboard_policy>>(ex, disc, plexus::node_id_from_name("esp32-onboard"), opts);
    auto &node = host->node();

    // The same public enumeration surface on-target (INV-1): the peerless onboard node yields an
    // empty snapshot, but the identical header-only participants() path runs byte-identically on xtensa.
    std::array<plexus::graph::participant_record, 4> peers{};
    const auto snapshot = node.participants(std::span<plexus::graph::participant_record>{peers});
    ESP_LOGI(k_tag, "onboard participants snapshot: count=%u truncated=%d", static_cast<unsigned>(snapshot.count), static_cast<int>(snapshot.truncated));

    plexus::subscriber<reading_codec> sub{
        node, "telemetry",
        [](const reading &v, const plexus::io::message_info &)
        {
            ESP_LOGI(k_tag, "onboard self-delivery: count=%u", static_cast<unsigned>(v.count));
        }};
    plexus::publisher<reading_codec> pub{node, "telemetry"};

    plexus::freertos::freertos_timer timer(ex);
    publish_loop loop{timer, pub};
    loop.arm();

    // The facade owns the cooperative loop discipline: it drains the executor (running the timer
    // fire, the posted self-delivery, and the subscriber callback), then parks watchdog-safe —
    // never returning. The inert poll handle carries no transport; the park floor keeps the idle
    // task fed so the loop cannot collapse into a watchdog-tripping spin.
    idle_poll idle;
    plexus::freertos::run(ex, idle);
}

}

extern "C" void app_main()
{
    // Spawn the user's plexus task with a stack sized to the engine's footprint, then return —
    // the spawned task runs the example for the lifetime of the device.
    xTaskCreate(plexus_task, "plexus", k_plexus_task_stack, nullptr, k_plexus_task_prio, nullptr);
}
