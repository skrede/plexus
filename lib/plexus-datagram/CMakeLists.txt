# Header-only datagram transport family: the mechanism-free fragmentation,
# reassembly, demux, and handshake framing/retransmit discipline shared by every
# message-oriented wire (udp, udpr, dtls). It is the middle tier of the transport
# module graph — the core pub/sub engine sits above it, the irreducible per-wire
# I/O mechanism below: a new datagram wire implements only its byte backend and
# reuses this family whole. The family is pure logic over the wire codec; it
# pulls in no asio, OpenSSL, OS, or RTOS header. Auto-discovered by the root glob.
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
