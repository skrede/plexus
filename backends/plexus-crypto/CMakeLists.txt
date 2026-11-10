# The only target that pulls OpenSSL in, so the header-only core stays crypto-free. The option is declared here, not
# at root, so the backend registers by adding this fragment alone.
option(PLEXUS_ENABLE_CRYPTO_BACKEND "Build the gated crypto backend (TLS plus DTLS over OpenSSL)" OFF)

if(NOT PLEXUS_ENABLE_CRYPTO_BACKEND)
    return()
endif()

include(${PROJECT_SOURCE_DIR}/cmake/plexus-deps.cmake)

find_package(OpenSSL REQUIRED)

# OBJECT, not STATIC: the OpenSSL TU also instantiates asio's io_context and internal mutex, and a second archive
# defining those COMDAT sections can fold against the asio backend's copy and SIGSEGV at process init. An OBJECT
# library folds its objects into each consumer's link unit, so asio's symbols are defined once per consumer.
add_library(plexus-crypto OBJECT
    src/tls_credential.cpp
    src/dtls_context.cpp
    src/dtls_cookie.cpp
    src/dtls_identity.cpp
    src/dtls_channel.cpp
    src/aead_cipher.cpp
    src/key_schedule.cpp)
add_library(plexus::crypto ALIAS plexus-crypto)

# PUBLIC so the OBJECT TU compiles against them too and every consumer inherits OpenSSL and the asio substrate.
target_include_directories(plexus-crypto PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)

target_link_libraries(plexus-crypto PUBLIC
    plexus::core
    plexus::wire
    plexus::stream
    plexus::asio
    plexus::datagram
    plexus_asio_dep
    OpenSSL::SSL
    OpenSSL::Crypto)

target_compile_features(plexus-crypto PUBLIC cxx_std_20)

set_target_properties(plexus-crypto PROPERTIES EXPORT_NAME crypto)

include(GNUInstallDirs)

# Without OBJECTS DESTINATION, CMake silently exports the OBJECT library as a bare INTERFACE and every crypto symbol
# goes undefined at an installed consumer's link. plexus_asio_dep is exported only with plexusAsioTargets, which the
# config package loads first, because exporting it twice trips CMake's "some-but-not-all targets" guard.
install(TARGETS plexus-crypto EXPORT plexusCryptoTargets
    OBJECTS DESTINATION ${CMAKE_INSTALL_LIBDIR}/plexus/objects)
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(EXPORT plexusCryptoTargets
    NAMESPACE plexus::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/plexus
    FILE plexus-crypto-targets.cmake)
