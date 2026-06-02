# Header-only generic core. Backends (asio, mbedTLS/OpenSSL) are separate gated targets.
add_library(plexus INTERFACE)
add_library(plexus::plexus ALIAS plexus)

target_include_directories(plexus INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)

target_compile_features(plexus INTERFACE cxx_std_20)
