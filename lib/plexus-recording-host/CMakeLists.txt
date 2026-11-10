# Ungated, so recording to a file works with the MCAP transcode OFF, and kept out of plexus-api because a
# std::filesystem sink cannot compile for the MCU target.
add_library(plexus-recording-host INTERFACE)
add_library(plexus::recording-host ALIAS plexus-recording-host)
set_target_properties(plexus-recording-host PROPERTIES EXPORT_NAME recording-host)

target_include_directories(plexus-recording-host INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)

target_link_libraries(plexus-recording-host INTERFACE plexus::core)

target_compile_features(plexus-recording-host INTERFACE cxx_std_20)

include(GNUInstallDirs)
install(TARGETS plexus-recording-host EXPORT plexusTargets)
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
