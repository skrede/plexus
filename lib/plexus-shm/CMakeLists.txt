# Header-only shared-memory transport family: the mechanism-free broadcast-ring,
# slot publish/subscribe, loan lifetime, and topic-registry discipline shared by
# every same-host zero-copy path. It is the middle tier of the transport module
# graph — the core pub/sub engine sits above it, the irreducible OS region/notify
# mechanism below: the family is templated over the region-broker and notifier
# concepts and a concrete backend (plexus::native) supplies the syscalls. The
# family is pure logic over those concepts; it pulls in no asio, OpenSSL, OS, or
# RTOS header. Auto-discovered by the root lib glob.
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
