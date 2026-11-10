add_library(plexus-freertos INTERFACE)
add_library(plexus::freertos ALIAS plexus-freertos)
set_target_properties(plexus-freertos PROPERTIES EXPORT_NAME freertos)

target_include_directories(plexus-freertos INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)

target_link_libraries(plexus-freertos INTERFACE plexus::core plexus::stream)

target_compile_features(plexus-freertos INTERFACE cxx_std_20)

include(GNUInstallDirs)
install(TARGETS plexus-freertos EXPORT plexusTargets)
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
