# plexus is header-only, so the component adds its include trees. The transport opens UART0, so the UART driver is this
# component's dependency, never the core's: the HAL stays out of lib/.
#
# The UART transport and the gate's type vocabulary are included from the sibling example directories, not copied: the
# transport is the link this device speaks, and the host gate compiles the same vocabulary header, so a drifted type id
# or name would break the very matching this example demonstrates.
idf_component_register(SRCS "app_main.cpp"
    INCLUDE_DIRS "." "${PLEXUS_ROOT}/examples/mcu" "${PLEXUS_ROOT}/examples/mcu/esp-idf/main"
    REQUIRES freertos esp_driver_uart)

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
