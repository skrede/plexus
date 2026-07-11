// On-hardware liveliness numerics sweep for the fixed-capacity peer-liveliness arbiter. For each
// cell in heartbeat_interval x heartbeat_miss_limit it drives a peer that heartbeats at real
// esp_timer boundaries to a steady alive verdict, then goes silent while the arbiter is evaluated on
// a fixed tick, and measures the wall time from the last heartbeat to the lost verdict (detection
// latency) plus any alive/lost oscillation (flaps). Each cell runs multiple times. One result line
// per run is written to UART2 (telemetry, GPIO17/GPIO16) and to the UART0 console; the host parses
// the telemetry, takes the per-cell median latency, and checks flaps == 0.

#include "plexus/io/peer_liveliness.h"
#include "plexus/io/liveliness_options.h"
#include "plexus/io/liveliness_peer_storage.h"

#include "plexus/node_id.h"

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"

#include "driver/uart.h"

#include "esp_timer.h"
#include "esp_heap_caps.h"
#include "esp_log.h"

#include <array>
#include <cstdio>
#include <cstddef>
#include <cstdint>
#include <cstring>

namespace {

constexpr const char *k_tag = "plexus-liveliness";

constexpr uart_port_t k_tlm_uart = UART_NUM_2;
constexpr int         k_tlm_tx    = 17;
constexpr int         k_tlm_rx    = 16;
constexpr int         k_tlm_baud  = 115200;

constexpr std::array<std::uint32_t, 3> k_intervals_ms{50, 100, 200};
constexpr std::array<std::uint32_t, 3> k_miss_limits{3, 5, 8};
constexpr int          k_runs_per_cell = 3;
constexpr int          k_warmup_beats  = 3;
constexpr std::uint32_t k_eval_tick_ms  = 10;

void tlm_open()
{
    const uart_config_t cfg{
        .baud_rate           = k_tlm_baud,
        .data_bits           = UART_DATA_8_BITS,
        .parity              = UART_PARITY_DISABLE,
        .stop_bits           = UART_STOP_BITS_1,
        .flow_ctrl           = UART_HW_FLOWCTRL_DISABLE,
        .rx_flow_ctrl_thresh = 0,
        .source_clk          = UART_SCLK_DEFAULT,
        .flags               = {},
    };
    uart_param_config(k_tlm_uart, &cfg);
    uart_set_pin(k_tlm_uart, k_tlm_tx, k_tlm_rx, UART_PIN_NO_CHANGE, UART_PIN_NO_CHANGE);
    uart_driver_install(k_tlm_uart, 1024, 1024, 0, nullptr, 0);
}

void tlm_line(const char *line)
{
    uart_write_bytes(k_tlm_uart, line, std::strlen(line));
    uart_write_bytes(k_tlm_uart, "\r\n", 2);
    ESP_LOGI(k_tag, "%s", line);
}

std::uint64_t now_ns()
{
    return static_cast<std::uint64_t>(esp_timer_get_time()) * 1000ull;
}

plexus::node_id peer_id(std::uint8_t tag)
{
    plexus::node_id id{};
    id[0] = static_cast<std::byte>(tag);
    return id;
}

struct run_result
{
    std::int64_t latency_ms;
    int          flaps;
};

// One sweep cell run: heartbeat to alive, then silence until the lost verdict. eval_ns feeds the
// callback the timestamp of the evaluate that fired the verdict, so first_lost_ns is the arbiter's
// own detection instant rather than a post-hoc read.
run_result run_cell(std::uint32_t interval_ms, std::uint32_t miss_limit)
{
    plexus::io::liveliness_options opts;
    opts.heartbeat_interval   = std::chrono::milliseconds(interval_ms);
    opts.heartbeat_miss_limit = miss_limit;

    plexus::io::peer_liveliness<plexus::io::fixed_liveliness_peer_storage<8>> arbiter{opts};

    int           alive_count   = 0;
    int           lost_count    = 0;
    bool          got_lost      = false;
    std::uint64_t eval_ns       = 0;
    std::uint64_t first_lost_ns = 0;
    arbiter.on_verdict(
        [&](const plexus::io::peer_liveliness_event &ev)
        {
            if(ev.verdict == plexus::io::liveliness_verdict::alive)
            {
                ++alive_count;
            }
            else
            {
                ++lost_count;
                if(!got_lost)
                {
                    got_lost      = true;
                    first_lost_ns = eval_ns;
                }
            }
        });
    arbiter.add_subscriber();

    const plexus::node_id id = peer_id(0x01);

    std::uint64_t last_hb_ns = 0;
    for(int i = 0; i < k_warmup_beats; ++i)
    {
        vTaskDelay(pdMS_TO_TICKS(interval_ms));
        last_hb_ns = now_ns();
        eval_ns    = last_hb_ns;
        arbiter.note_heartbeat(id, last_hb_ns);
        arbiter.evaluate(last_hb_ns);
    }

    const std::uint64_t deadline_ns =
            last_hb_ns + static_cast<std::uint64_t>(miss_limit) * interval_ms * 4ull * 1'000'000ull;
    while(!got_lost)
    {
        vTaskDelay(pdMS_TO_TICKS(k_eval_tick_ms));
        eval_ns = now_ns();
        arbiter.evaluate(eval_ns);
        if(eval_ns > deadline_ns)
            break;
    }

    const std::int64_t latency_ms =
            got_lost ? static_cast<std::int64_t>((first_lost_ns - last_hb_ns) / 1'000'000ull) : -1;
    const int flaps = (alive_count > 0 ? alive_count - 1 : 0) + (lost_count > 0 ? lost_count - 1 : 0);
    return run_result{latency_ms, flaps};
}

void sweep_task(void *)
{
    tlm_open();
    vTaskDelay(pdMS_TO_TICKS(300));

    char line[160];
    std::snprintf(line, sizeof(line), "SWEEP START heap_free=%u",
                  static_cast<unsigned>(esp_get_free_heap_size()));
    tlm_line(line);

    for(std::uint32_t interval_ms : k_intervals_ms)
        for(std::uint32_t miss : k_miss_limits)
            for(int run = 0; run < k_runs_per_cell; ++run)
            {
                const run_result r = run_cell(interval_ms, miss);
                std::snprintf(line, sizeof(line),
                              "cell interval=%u miss=%u run=%d latency_ms=%lld flaps=%d",
                              static_cast<unsigned>(interval_ms), static_cast<unsigned>(miss), run,
                              static_cast<long long>(r.latency_ms), r.flaps);
                tlm_line(line);
            }

    std::snprintf(line, sizeof(line), "SWEEP DONE heap_free=%u",
                  static_cast<unsigned>(esp_get_free_heap_size()));
    tlm_line(line);

    for(;;)
        vTaskDelay(pdMS_TO_TICKS(1000));
}

}

extern "C" void app_main()
{
    xTaskCreate(sweep_task, "sweep", 8192, nullptr, 5, nullptr);
}
