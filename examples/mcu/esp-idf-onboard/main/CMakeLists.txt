# plexus is header-only, so the component adds its include trees. The only transport is the intra-node self-route and
# the payload is an in-process counter, so no driver is required and no HAL is touched.
idf_component_register(SRCS "app_main.cpp" INCLUDE_DIRS "."
    REQUIRES freertos)

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
