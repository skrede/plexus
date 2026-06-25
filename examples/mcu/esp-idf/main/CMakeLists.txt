# The plexus-freertos serial example component. plexus is header-only, so this pulls in its
# include trees and binds against FreeRTOS plus the UART driver (the transport opens UART0)
# and the GPIO driver (the example pin read). Both drivers are EXAMPLE-component deps, never
# core — the HAL touch stays out of lib/ (the pure-comms-library invariant).
idf_component_register(SRCS "app_main.cpp" INCLUDE_DIRS "."
    REQUIRES freertos esp_driver_uart esp_driver_gpio)

target_include_directories(${COMPONENT_LIB} PRIVATE
    "${PLEXUS_ROOT}/lib/plexus-core/include"
    "${PLEXUS_ROOT}/lib/plexus-wire/include"
    "${PLEXUS_ROOT}/lib/plexus-stream/include"
    "${PLEXUS_ROOT}/lib/plexus-api/include"
    "${PLEXUS_ROOT}/backends/plexus-freertos/include")

# Override IDF's default C++17 to gnu++20, exceptions + RTTI off. gnu++20 leaves
# __cpp_lib_move_only_function undefined, selecting the compat.h SBO fallback.
target_compile_options(${COMPONENT_LIB} PRIVATE -std=gnu++20 -fno-exceptions -fno-rtti)
