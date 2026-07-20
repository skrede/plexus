# The topic-propagation example component. plexus is header-only, so this pulls in its include
# trees and binds against FreeRTOS plus the UART driver (the transport opens UART0) — an EXAMPLE
# dep, never core: the HAL touch stays out of lib/ (the pure-comms-library invariant).
#
# The UART transport and the gate's shared type vocabulary are included from the sibling example
# dirs rather than copied: the transport is the same link this device speaks, and the vocabulary
# MUST be one definition — the host gate on the far side of the cable compiles the same header, and
# a drifted type id or name would break the very matching this example exists to demonstrate.
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

# Override IDF's default C++17 to gnu++20, exceptions + RTTI off. gnu++20 leaves
# __cpp_lib_move_only_function undefined, selecting the compat.h SBO fallback.
target_compile_options(${COMPONENT_LIB} PRIVATE -std=gnu++20 -fno-exceptions -fno-rtti)
