idf_component_register(SRCS "app_main.cpp" INCLUDE_DIRS "."
    REQUIRES freertos esp_wifi esp_netif nvs_flash esp_event esp_timer esp_driver_uart lwip)

target_include_directories(${COMPONENT_LIB} PRIVATE
    "${PLEXUS_ROOT}/lib/plexus-core/include"
    "${PLEXUS_ROOT}/lib/plexus-wire/include"
    "${PLEXUS_ROOT}/lib/plexus-stream/include"
    "${PLEXUS_ROOT}/lib/plexus-api/include"
    "${PLEXUS_ROOT}/backends/plexus-freertos/include")

target_compile_options(${COMPONENT_LIB} PRIVATE -std=gnu++20 -fno-exceptions -fno-rtti)

# The bench cell is selected at build time: BENCH_TRANSPORT picks the transport+receive policy so
# every cell runs the identical workload. The runner sets it per cell; an unset build defaults to
# the lwIP-P2 cell (a plain build stays a valid runnable). PLEXUS_HOST_ENDPOINT is repointed at the
# host's address at run time; the committed default is a placeholder, never the test IP.
if(DEFINED BENCH_TRANSPORT)
    if(BENCH_TRANSPORT STREQUAL "serial")
        target_compile_definitions(${COMPONENT_LIB} PRIVATE BENCH_SERIAL)
    elseif(BENCH_TRANSPORT STREQUAL "lwip-p1")
        target_compile_definitions(${COMPONENT_LIB} PRIVATE BENCH_LWIP_P1)
    elseif(BENCH_TRANSPORT STREQUAL "lwip-p2")
        target_compile_definitions(${COMPONENT_LIB} PRIVATE BENCH_LWIP_P2)
    endif()
endif()

# The bench workload is selected at build time: BENCH_WORKLOAD picks latency (request/echo round trips,
# the default when unset), oneway (a saturating device->host stream), or pipeline (K round trips in flight).
# The runner sets it per cell; BENCH_WINDOW_K sizes the pipeline window (default 1 in the firmware).
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

# The one-way widen-cost variant: raise the egress cap to the deep value to measure its throughput cost.
if(DEFINED BENCH_DEEP_EGRESS)
    target_compile_definitions(${COMPONENT_LIB} PRIVATE BENCH_DEEP_EGRESS)
endif()

if(DEFINED PLEXUS_HOST_ENDPOINT)
    target_compile_definitions(${COMPONENT_LIB} PRIVATE PLEXUS_HOST_ENDPOINT="${PLEXUS_HOST_ENDPOINT}")
endif()
