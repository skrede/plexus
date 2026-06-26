idf_component_register(SRCS "app_main.cpp" INCLUDE_DIRS "."
    REQUIRES freertos esp_wifi esp_netif nvs_flash esp_event lwip)

target_include_directories(${COMPONENT_LIB} PRIVATE
    "${PLEXUS_ROOT}/lib/plexus-core/include"
    "${PLEXUS_ROOT}/lib/plexus-wire/include"
    "${PLEXUS_ROOT}/lib/plexus-stream/include"
    "${PLEXUS_ROOT}/lib/plexus-api/include"
    "${PLEXUS_ROOT}/backends/plexus-freertos/include")

target_compile_options(${COMPONENT_LIB} PRIVATE -std=gnu++20 -fno-exceptions -fno-rtti)
