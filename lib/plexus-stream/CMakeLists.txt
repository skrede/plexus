# Pure logic over the wire codec that pulls in no asio, OpenSSL, OS or RTOS header, so a new stream wire implements
# only its byte backend.
add_library(plexus-stream INTERFACE)
add_library(plexus::stream ALIAS plexus-stream)
set_target_properties(plexus-stream PROPERTIES EXPORT_NAME stream)

target_include_directories(plexus-stream INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)

target_link_libraries(plexus-stream INTERFACE plexus::core plexus::wire)

target_compile_features(plexus-stream INTERFACE cxx_std_20)

include(GNUInstallDirs)
install(TARGETS plexus-stream EXPORT plexusTargets)
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
