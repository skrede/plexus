# The onboard intra-node zero-copy benchmark component. plexus is header-only, so this pulls in its
# include trees and binds against FreeRTOS only — there is NO transport driver: the node's sole
# transport is the intra-node self-route, so no UART, no lwIP, no GPIO touch. esp_timer supplies the
# microsecond round-trip clock; the heap reading comes from the default heap component.
idf_component_register(SRCS "app_main.cpp" INCLUDE_DIRS "."
    REQUIRES freertos esp_timer)

target_include_directories(${COMPONENT_LIB} PRIVATE
    "${PLEXUS_ROOT}/lib/plexus-core/include"
    "${PLEXUS_ROOT}/lib/plexus-wire/include"
    "${PLEXUS_ROOT}/lib/plexus-stream/include"
    "${PLEXUS_ROOT}/lib/plexus-api/include"
    "${PLEXUS_ROOT}/backends/plexus-freertos/include")

# Override IDF's default C++17 to gnu++20, exceptions + RTTI off. gnu++20 leaves
# __cpp_lib_move_only_function undefined, selecting the compat.h SBO fallback.
target_compile_options(${COMPONENT_LIB} PRIVATE -std=gnu++20 -fno-exceptions -fno-rtti)
