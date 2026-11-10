add_library(plexus-inproc INTERFACE)
add_library(plexus::inproc ALIAS plexus-inproc)
set_target_properties(plexus-inproc PROPERTIES EXPORT_NAME inproc)

target_include_directories(plexus-inproc INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)

target_link_libraries(plexus-inproc INTERFACE plexus::core)

target_compile_features(plexus-inproc INTERFACE cxx_std_20)

include(GNUInstallDirs)
install(TARGETS plexus-inproc EXPORT plexusTargets)
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
