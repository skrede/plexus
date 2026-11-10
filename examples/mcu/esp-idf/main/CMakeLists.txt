# plexus is header-only, so the component adds its include trees. The transport opens UART0 and the example reads a
# pin, so the UART and GPIO drivers are this component's dependencies, never the core's: the HAL stays out of lib/.
# examples/mcu is on the include path because the payload cap lives in serial_link.h, which the host gate compiles
# too, so the two ends cannot drift apart.
idf_component_register(SRCS "app_main.cpp" INCLUDE_DIRS "." "${PLEXUS_ROOT}/examples/mcu"
    REQUIRES freertos esp_driver_uart esp_driver_gpio)

target_include_directories(${COMPONENT_LIB} PRIVATE
    "${PLEXUS_ROOT}/lib/plexus-core/include"
    "${PLEXUS_ROOT}/lib/plexus-wire/include"
    "${PLEXUS_ROOT}/lib/plexus-stream/include"
    "${PLEXUS_ROOT}/lib/plexus-discovery/include"
    "${PLEXUS_ROOT}/lib/plexus-api/include"
    "${PLEXUS_ROOT}/backends/plexus-freertos/include")

# Overrides IDF's C++17 default. gnu++20 leaves __cpp_lib_move_only_function undefined, which selects the compat.h SBO
# fallback.
target_compile_options(${COMPONENT_LIB} PRIVATE -std=gnu++20 -fno-exceptions -fno-rtti)
