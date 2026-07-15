// The on-device half of the topic-propagation example: a node over the real UART link that
// PRODUCES a typed topic and CONSUMES the host's, then answers the graph question about its own
// peer — which topics does the far side produce, and under which declared type name. The device
// publishes that answer back over the same link, so the host gate can assert the device's view
// without a console (plexus owns UART0 here, as in the serial example).
//
// Nothing about enumeration is device-specific: the query surface, the records, and the table the
// declarations fold into are the same core headers the host compiles, selected by template
// parameter and never by a platform #ifdef. That is what this firmware's cross-compile proves.
//
// The node lives off the task stack — heap-allocated once at setup so the constrained main stack
// need not carry the engine. The composition runs in a task the example creates and drives through
// the device facade; there is no background plexus thread.

#include "uart_transport.h"

#include "topic_graph_types.h"

#include "plexus/node.h"
#include "plexus/publisher.h"
#include "plexus/subscriber.h"
#include "plexus/node_options.h"

#include "plexus/graph/topic_record.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/message_info.h"
#include "plexus/io/reconnect_config.h"

#include "plexus/freertos/device_runtime.h"
#include "plexus/freertos/freertos_timer.h"
#include "plexus/freertos/freertos_executor.h"

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"

#include <span>
#include <array>
#include <memory>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <system_error>

namespace {

// Sized as the serial example's: its measured peak under live host traffic was ~3968 bytes, and
// this composition adds only the table sweep. ESP32 services peripheral ISRs on the active task's
// stack, so the budget absorbs worst-case interrupt nesting on top of the deepest plexus path.
constexpr std::uint32_t k_plexus_task_stack = 8192; // bytes (ESP-IDF xTaskCreate takes bytes)
constexpr UBaseType_t k_plexus_task_prio    = 5;

// The device sits on both its own topics and the host's, so the sweep must hold every edge the
// four-topic composition can produce before the report can trust a miss to be a real miss.
constexpr std::size_t k_edge_scratch = 8;

using device_node = plexus::node<example::uart_policy, example::uart_transport>;

// The periodic report loop: sweep the node's own enumeration surface for the type name the HOST
// declared on its topic, and publish that name back. An empty name means the device enumerates no
// such producer — the host gate reads that as the propagation miss it is, rather than a timeout it
// has to guess about. arm() installs a FRESH handler each cycle (each async_wait move-constructs a
// new lambda, the move-only-safe analog of the host's steady_timer loop).
struct report_loop
{
    plexus::freertos::freertos_timer &timer;
    device_node &node;
    plexus::publisher<example::reading_codec> &telemetry;
    plexus::publisher<void> &report;
    std::uint32_t next{};

    void arm()
    {
        timer.expires_after(std::chrono::milliseconds{500});
        timer.async_wait(
            [this](std::error_code)
            {
                telemetry.publish(example::counter{next++});
                publish_view();
                arm();
            });
    }

    void publish_view()
    {
        std::array<plexus::graph::topic_record, k_edge_scratch> edges{};
        const auto type_name = example::declared_type_of(node, example::k_host_topic, plexus::graph::topic_role::publisher,
                                                         std::span<plexus::graph::topic_record>{edges});
        report.publish(std::as_bytes(std::span<const char>{type_name.data(), type_name.size()}));
    }
};

// The user's one task: it owns the executor and the transport, then hands the executor to the
// device facade to drive. ex/transport/disc/opts are task-scope locals declared ABOVE the node —
// the node borrows them by reference, so they must outlive it.
void plexus_task(void *)
{
    using namespace std::chrono_literals;

    plexus::freertos::freertos_executor ex;
    example::uart_transport transport; // opens UART0 and delivers the one uart_channel

    // Point-at-port discovery: the serial link is the only peer, so the table is empty.
    plexus::discovery::static_discovery disc{{}};

    plexus::node_options opts;
    opts.name              = "esp32-topic-graph";
    opts.max_message_bytes = example::k_max_payload_bytes; // dialed small for the target
    opts.reconnect         = plexus::io::reconnect_config{200ms, 5s, std::nullopt, std::nullopt};
    opts.redial_seed       = 0x32C0DE;

    auto node = std::make_unique<device_node>(ex, disc, "esp32-topic-graph", transport, opts);
    // This device LISTENS; by convention the dialing peer (the host gate) drives the handshake.
    node->listen({"serial", "uart0"});

    plexus::publisher<example::reading_codec> telemetry{*node, example::k_device_topic};
    plexus::publisher<void> report{*node, example::k_report_topic};
    plexus::subscriber<example::command_codec> commands{
        *node, example::k_host_topic,
        [](const example::counter &, const plexus::io::message_info &) {}};

    plexus::freertos::freertos_timer timer(ex);
    report_loop loop{timer, *node, telemetry, report};
    loop.arm();

    // The facade owns the cooperative loop discipline: poll the UART into ready work, drain the
    // executor, then park watchdog-safe — never returning.
    plexus::freertos::run(ex, transport);
}

}

extern "C" void app_main()
{
    xTaskCreate(plexus_task, "plexus", k_plexus_task_stack, nullptr, k_plexus_task_prio, nullptr);
}
