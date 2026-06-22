# The plexus-mcu compile-proof component. plexus is header-only, so this pulls in
# its include trees and binds against FreeRTOS only.
idf_component_register(SRCS "app_main.cpp" INCLUDE_DIRS "." REQUIRES freertos)

target_include_directories(${COMPONENT_LIB} PRIVATE
    "${PLEXUS_ROOT}/lib/plexus-core/include"
    "${PLEXUS_ROOT}/lib/plexus-wire/include"
    "${PLEXUS_ROOT}/lib/plexus-mcu/include")

# LOCKED: override IDF's default C++17 to gnu++20, exceptions + RTTI off. gnu++20
# leaves __cpp_lib_move_only_function undefined, selecting the compat.h SBO fallback.
target_compile_options(${COMPONENT_LIB} PRIVATE -std=gnu++20 -fno-exceptions -fno-rtti)
