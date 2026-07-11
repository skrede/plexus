idf_component_register(SRCS "app_main.cpp" INCLUDE_DIRS "."
    REQUIRES freertos esp_timer esp_driver_uart esp_hw_support)

target_include_directories(${COMPONENT_LIB} PRIVATE
    "${PLEXUS_ROOT}/lib/plexus-core/include")

target_compile_options(${COMPONENT_LIB} PRIVATE -std=gnu++20 -fno-exceptions -fno-rtti)
