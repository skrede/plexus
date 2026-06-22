// The on-device serial vertical slice: the engine running on the constrained-target
// substrate through example application code. It constructs the cooperative executor, the
// real UART byte_channel (delivered by the example-side uart_transport), a node, and a
// timer that samples the BOOT button (GPIO0) and publishes the reading through the real
// handshake + framing + CRC + pub/sub engine. The HAL touch — the pin read and its
// driver/gpio.h include — lives ONLY here in the example component, never in lib/ (the
// pure-comms-library invariant): plexus sees opaque bytes, no HAL/policy leaks into core.
//
// The super-loop never returns: the user's one task drives the executor with no background
// plexus thread. The block-with-timeout park is LOAD-BEARING — it yields to the FreeRTOS
// idle task and feeds the task watchdog; a busy spin here trips a watchdog reset. The tick
// stays 100 Hz (CONFIG_FREERTOS_HZ is NOT raised). plexus owns UART0 (console NONE); the
// magic-byte resync in the framing layer absorbs the un-suppressable boot-ROM preamble.

#include "uart_transport.h"

#include "plexus/node.h"
#include "plexus/publisher.h"
#include "plexus/node_options.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/reconnect_config.h"

#include "plexus/mcu/freertos_timer.h"
#include "plexus/mcu/freertos_executor.h"

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"

#include "driver/gpio.h"

#include <span>
#include <array>
#include <chrono>
#include <cstddef>
#include <optional>
#include <system_error>

namespace {

// GPIO0 is the BOOT button: released = 1, pressed = 0 — a deterministic digital level for a
// reproducible multi-run gate assert. Configure it as a plain input ONCE before sampling.
constexpr gpio_num_t k_sample_pin = GPIO_NUM_0;

void configure_sample_pin()
{
    const gpio_config_t cfg{
            .pin_bit_mask = 1ULL << k_sample_pin,
            .mode         = GPIO_MODE_INPUT,
            .pull_up_en   = GPIO_PULLUP_DISABLE,
            .pull_down_en = GPIO_PULLDOWN_DISABLE,
            .intr_type    = GPIO_INTR_DISABLE,
    };
    gpio_config(&cfg);
}

// The INV-2 boundary: the one HAL touch. Read the pin level (example code only) and hand
// the single byte to plexus as opaque payload — plexus never sees the HAL, only the bytes.
std::array<std::byte, 1> sample_reading()
{
    const int level = gpio_get_level(k_sample_pin);
    return {static_cast<std::byte>(level & 0xff)};
}

// The periodic sample-and-publish loop. arm() installs a FRESH handler each cycle that
// samples the pin, publishes the byte, and re-arms via arm() again — a self-rescheduling
// timer without copying a move-only handler (each async_wait move-constructs a new lambda,
// the move-only-function-safe analog of the host steady_timer publish loop).
struct sample_loop
{
    plexus::mcu::freertos_timer &timer;
    plexus::publisher<void>     &topic;

    void arm()
    {
        timer.expires_after(std::chrono::milliseconds{1000});
        timer.async_wait(
                [this](std::error_code)
                {
                    const auto reading = sample_reading();
                    topic.publish(std::span<const std::byte>{reading});
                    arm();
                });
    }
};

}

extern "C" void app_main()
{
    using namespace std::chrono_literals;

    plexus::mcu::freertos_executor ex;
    example::uart_transport        transport; // opens UART0 and delivers the one uart_channel

    // Point-at-port discovery: the serial link is the only peer, so the table is empty (no
    // IP discovery). The node is discoverable from birth and the serial channel is the link.
    plexus::discovery::static_discovery disc{{}};

    plexus::node_options opts;
    opts.name              = "esp32-telemetry";
    opts.max_message_bytes = example::k_max_payload_bytes; // dialed small for the target
    opts.reconnect         = plexus::io::reconnect_config{200ms, 5s, std::nullopt, std::nullopt};
    opts.redial_seed       = 0x32C0DE;

    plexus::node<example::uart_policy, example::uart_transport> node{ex, disc, "esp32-telemetry",
                                                                     transport, opts};
    // This device LISTENS; by convention the dialing peer (the host gate) drives the
    // handshake request. The endpoint scheme matches the transport's "serial" advertisement.
    node.listen({"serial", "uart0"});

    plexus::publisher<void> telemetry{node, "telemetry"};

    // The cooperative timer samples the pin and publishes, then re-arms itself.
    plexus::mcu::freertos_timer timer(ex);
    sample_loop                 loop{timer, telemetry};
    configure_sample_pin();
    loop.arm();

    for(;;)
    {
        // The NON-BLOCKING UART drain step: transport.poll() forwards to the delivered
        // channel.poll() — uart_read_bytes(...,0) -> decorator.feed -> on_data -> engine. The
        // engine owns the channel; the transport keeps a non-owning handle to poll it here.
        transport.poll();

        // Drain ALL ready work the poll (and the timer) produced. pump() never blocks.
        while(ex.pump())
        {
        }

        // QUIESCENT: the UNCHANGED block-with-timeout park feeds the task watchdog and yields
        // to the idle task. The timeout bounds the wait so the armed timer still fires on
        // schedule. Never a busy-poll: a spin here starves the idle task into a watchdog reset.
        vTaskDelay(pdMS_TO_TICKS(10));
    }
}
