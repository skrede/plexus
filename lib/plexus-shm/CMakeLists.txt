# Templated over the region-broker and notifier concepts, with a concrete backend (plexus::native) supplying the
# syscalls, so the family pulls in no asio, OpenSSL, OS or RTOS header.
add_library(plexus-shm INTERFACE)
add_library(plexus::shm ALIAS plexus-shm)
set_target_properties(plexus-shm PROPERTIES EXPORT_NAME shm)

target_include_directories(plexus-shm INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)

target_link_libraries(plexus-shm INTERFACE plexus::core plexus::wire)

target_compile_features(plexus-shm INTERFACE cxx_std_20)

include(GNUInstallDirs)
install(TARGETS plexus-shm EXPORT plexusTargets)
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
