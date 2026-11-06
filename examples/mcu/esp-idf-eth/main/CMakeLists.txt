idf_component_register(SRCS "app_main.cpp" INCLUDE_DIRS "."
    REQUIRES freertos esp_eth esp_netif esp_event lwip)

target_include_directories(${COMPONENT_LIB} PRIVATE
    "${PLEXUS_ROOT}/lib/plexus-core/include"
    "${PLEXUS_ROOT}/lib/plexus-wire/include"
    "${PLEXUS_ROOT}/lib/plexus-stream/include"
    "${PLEXUS_ROOT}/lib/plexus-discovery/include"
    "${PLEXUS_ROOT}/lib/plexus-api/include"
    "${PLEXUS_ROOT}/backends/plexus-freertos/include")

target_compile_options(${COMPONENT_LIB} PRIVATE -std=gnu++20 -fno-exceptions -fno-rtti)

# The gate script sets these per run; an unset variable keeps the firmware's built-in default.
if(DEFINED PLEXUS_HOST_ENDPOINT)
    target_compile_definitions(${COMPONENT_LIB} PRIVATE PLEXUS_HOST_ENDPOINT="${PLEXUS_HOST_ENDPOINT}")
endif()

if(DEFINED PLEXUS_DEAD_ENDPOINT)
    target_compile_definitions(${COMPONENT_LIB} PRIVATE PLEXUS_DEAD_ENDPOINT="${PLEXUS_DEAD_ENDPOINT}")
endif()

if(DEFINED PLEXUS_PUBLISH_PERIOD_MS)
    target_compile_definitions(${COMPONENT_LIB} PRIVATE PLEXUS_PUBLISH_PERIOD_MS=${PLEXUS_PUBLISH_PERIOD_MS})
endif()

if(PLEXUS_RX_TASK)
    target_compile_definitions(${COMPONENT_LIB} PRIVATE PLEXUS_RX_TASK)
endif()
