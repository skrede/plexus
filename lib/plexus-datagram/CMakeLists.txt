# Pure logic over the wire codec that pulls in no asio, OpenSSL, OS or RTOS header, so a new datagram wire implements
# only its byte backend.
add_library(plexus-datagram INTERFACE)
add_library(plexus::datagram ALIAS plexus-datagram)
set_target_properties(plexus-datagram PROPERTIES EXPORT_NAME datagram)

target_include_directories(plexus-datagram INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)

target_link_libraries(plexus-datagram INTERFACE plexus::core plexus::wire)

target_compile_features(plexus-datagram INTERFACE cxx_std_20)

include(GNUInstallDirs)
install(TARGETS plexus-datagram EXPORT plexusTargets)
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
