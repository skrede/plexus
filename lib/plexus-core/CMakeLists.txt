# Header-only minimal engine: the generic core that pairs with the wire codec.
# Backends (asio, mbedTLS/OpenSSL) are separate gated targets; the consumable
# facade is plexus-api. Core headers include plexus/wire/* today, so the wire
# link is folded in here to keep the target self-contained.
add_library(plexus-core INTERFACE)
add_library(plexus::core ALIAS plexus-core)
set_target_properties(plexus-core PROPERTIES EXPORT_NAME core)

target_include_directories(plexus-core INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)

target_link_libraries(plexus-core INTERFACE plexus::wire)

target_compile_features(plexus-core INTERFACE cxx_std_20)
