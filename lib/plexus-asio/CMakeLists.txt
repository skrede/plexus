# Gated, compiled asio TCP backend: the production transport half of the slice.
# The header-only core (plexus, plexus-wire) never links asio — this target, and
# only this target, pulls standalone asio in, and only when the backend is ON.
# Auto-discovered by the root lib glob; self-gated so the core stays asio-free
# when PLEXUS_ENABLE_ASIO_BACKEND is OFF.
if(NOT PLEXUS_ENABLE_ASIO_BACKEND)
    return()
endif()

# Shared, pinned standalone-asio dependency (one copy across plexus + mdnspp).
include(${PROJECT_SOURCE_DIR}/cmake/plexus-deps.cmake)

add_library(plexus-asio INTERFACE)
add_library(plexus::asio ALIAS plexus-asio)

target_include_directories(plexus-asio INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)

target_link_libraries(plexus-asio INTERFACE
    plexus::plexus
    plexus::wire
    plexus_asio_dep)

target_compile_features(plexus-asio INTERFACE cxx_std_20)
