# examples/mcu is on the include path because the serial cell's payload cap lives in serial_link.h,
# which serial_bench_host compiles too, so the two ends cannot drift apart.
idf_component_register(SRCS "app_main.cpp" INCLUDE_DIRS "." "${PLEXUS_ROOT}/examples/mcu"
    REQUIRES freertos esp_wifi esp_netif nvs_flash esp_event esp_timer esp_driver_uart lwip)

target_include_directories(${COMPONENT_LIB} PRIVATE
    "${PLEXUS_ROOT}/lib/plexus-core/include"
    "${PLEXUS_ROOT}/lib/plexus-wire/include"
    "${PLEXUS_ROOT}/lib/plexus-stream/include"
    "${PLEXUS_ROOT}/lib/plexus-discovery/include"
    "${PLEXUS_ROOT}/lib/plexus-api/include"
    "${PLEXUS_ROOT}/backends/plexus-freertos/include")

target_compile_options(${COMPONENT_LIB} PRIVATE -std=gnu++20 -fno-exceptions -fno-rtti)

# The runner picks each cell's transport and receive policy at build time, so every cell runs the same workload. An
# unset BENCH_TRANSPORT builds the lwIP-P2 cell, so a plain build stays runnable.
if(DEFINED BENCH_TRANSPORT)
    if(BENCH_TRANSPORT STREQUAL "serial")
        target_compile_definitions(${COMPONENT_LIB} PRIVATE BENCH_SERIAL)
    elseif(BENCH_TRANSPORT STREQUAL "lwip-p1")
        target_compile_definitions(${COMPONENT_LIB} PRIVATE BENCH_LWIP_P1)
    elseif(BENCH_TRANSPORT STREQUAL "lwip-p2")
        target_compile_definitions(${COMPONENT_LIB} PRIVATE BENCH_LWIP_P2)
    endif()
endif()

# BENCH_WORKLOAD: latency (request/echo round trips, the default), oneway (a saturating device-to-host stream) or
# pipeline (BENCH_WINDOW_K round trips in flight, default 1 in the firmware).
if(DEFINED BENCH_WORKLOAD)
    if(BENCH_WORKLOAD STREQUAL "oneway")
        target_compile_definitions(${COMPONENT_LIB} PRIVATE BENCH_WORKLOAD_ONEWAY)
    elseif(BENCH_WORKLOAD STREQUAL "pipeline")
        target_compile_definitions(${COMPONENT_LIB} PRIVATE BENCH_WORKLOAD_PIPELINE)
    endif()
endif()

if(DEFINED BENCH_WINDOW_K)
    target_compile_definitions(${COMPONENT_LIB} PRIVATE BENCH_WINDOW_K=${BENCH_WINDOW_K})
endif()

# Device and host must agree on the baud (default 115200 in bench_uart.h) or the framing desyncs, so the runner starts
# the host serial peer at the same baud.
if(DEFINED BENCH_BAUD)
    target_compile_definitions(${COMPONENT_LIB} PRIVATE BENCH_BAUD=${BENCH_BAUD})
endif()

# Raises the egress cap to the deep value, to measure what widening it costs in one-way throughput.
if(DEFINED BENCH_DEEP_EGRESS)
    target_compile_definitions(${COMPONENT_LIB} PRIVATE BENCH_DEEP_EGRESS)
endif()

# The runner points this at the host's address; the firmware's committed default is a placeholder, never the test IP.
if(DEFINED PLEXUS_HOST_ENDPOINT)
    target_compile_definitions(${COMPONENT_LIB} PRIVATE PLEXUS_HOST_ENDPOINT="${PLEXUS_HOST_ENDPOINT}")
endif()
