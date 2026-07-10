# Header-only discovery family: the concrete discovery mechanisms that sit above
# the discovery contract (the discovery ABC + options stay in core, included by
# the api). It holds the multicast leaf, the socket-free static fallback, and the
# leaf-only card codecs — pure logic over the wire codec, pulling in no asio,
# OpenSSL, OS, or RTOS header. The one-way arrow holds: core and api link no
# discovery; only a backend or a test that compiles a concrete leaf reaches here.
# Auto-discovered by the root glob.
add_library(plexus-discovery INTERFACE)
add_library(plexus::discovery ALIAS plexus-discovery)
set_target_properties(plexus-discovery PROPERTIES EXPORT_NAME discovery)

target_include_directories(plexus-discovery INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)

target_link_libraries(plexus-discovery INTERFACE plexus::core plexus::wire)

target_compile_features(plexus-discovery INTERFACE cxx_std_20)

include(GNUInstallDirs)
install(TARGETS plexus-discovery EXPORT plexusTargets)
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
