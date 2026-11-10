# Pure logic over the wire codec that pulls in no asio, OpenSSL, OS or RTOS header. The discovery contract stays in
# core, so core and api never link this; only a backend or a test that compiles a concrete leaf does.
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
