# Header-only wire layer: the pure-byte codec, frame protocol, and the
# out-of-band-hardened stream reassembler. Zero dependencies.
add_library(plexus-wire INTERFACE)
add_library(plexus::wire ALIAS plexus-wire)
set_target_properties(plexus-wire PROPERTIES EXPORT_NAME wire)

target_include_directories(plexus-wire INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)

target_compile_features(plexus-wire INTERFACE cxx_std_20)

include(GNUInstallDirs)
install(TARGETS plexus-wire EXPORT plexusTargets)
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
