# Header-only FreeRTOS Policy substrate for the constrained-target floor: the
# cooperative super-loop executor and the tick-backed poll-fired timer, plus the
# host-API shim that lets the same source compile in the host suite and
# cross-build against the real kernel on-device. A leaf substrate for the Policy
# seam, depending only on the core. Auto-discovered by the root lib glob.
add_library(plexus-mcu INTERFACE)
add_library(plexus::mcu ALIAS plexus-mcu)
set_target_properties(plexus-mcu PROPERTIES EXPORT_NAME mcu)

target_include_directories(plexus-mcu INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)

target_link_libraries(plexus-mcu INTERFACE plexus::core plexus::stream)

target_compile_features(plexus-mcu INTERFACE cxx_std_20)

include(GNUInstallDirs)
install(TARGETS plexus-mcu EXPORT plexusTargets)
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
