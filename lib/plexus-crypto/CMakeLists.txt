# Gated, compiled crypto backend (TLS-over-TCP plus DTLS-over-UDP): plexus's
# crypto transports. The header-only core (plexus, plexus-wire) never links
# OpenSSL or asio — this target, and only this target, pulls OpenSSL in, and only
# when the backend is ON. Auto-discovered by the root lib glob; self-gated so the
# core stays crypto-free when PLEXUS_ENABLE_CRYPTO_BACKEND is OFF.
#
# The option is declared HERE (not in the root CMakeLists.txt) so the target
# registers by adding this fragment alone, matching the discovery-adapter
# precedent.
option(PLEXUS_ENABLE_CRYPTO_BACKEND "Build the gated crypto backend (TLS plus DTLS over OpenSSL)" OFF)

if(NOT PLEXUS_ENABLE_CRYPTO_BACKEND)
    return()
endif()

# Shared, pinned standalone-asio dependency (one copy across every consumer).
include(${PROJECT_SOURCE_DIR}/cmake/plexus-deps.cmake)

# The host distro's OpenSSL (libssl/libcrypto + headers) resolved by CMake's
# bundled FindOpenSSL — no language-registry package is fetched.
find_package(OpenSSL REQUIRED)

# OBJECT library — deliberately NOT STATIC. The single OpenSSL-touching TU also
# transitively instantiates asio's io_context / internal mutex. A second STATIC
# archive that defines those identical COMDAT sections can be folded against the
# asio backend's copy by the linker and collide at process init (a SIGSEGV that
# reproduces only at runtime, never at compile). An OBJECT library compiles the
# TU once but folds its objects DIRECTLY into each consumer's link unit (via the
# INTERFACE usage requirements below + $<TARGET_OBJECTS:plexus-crypto> on the
# consumer's source list), so asio's symbols are defined in exactly one link
# unit per consumer — never a second archive. This is the load-bearing reason
# the kind is OBJECT, proven by a live link+run, not a green build alone.
add_library(plexus-crypto OBJECT
    src/tls_credential.cpp
    src/dtls_context.cpp
    src/dtls_cookie.cpp
    src/dtls_identity.cpp
    src/dtls_channel.cpp
    src/aead_cipher.cpp
    src/key_schedule.cpp)
add_library(plexus::crypto ALIAS plexus-crypto)

# Headers are exposed as INTERFACE include dirs (PUBLIC so the OBJECT TU itself
# compiles against them too); the link deps are carried as INTERFACE so every
# consumer that links plexus::crypto inherits OpenSSL + the asio substrate.
target_include_directories(plexus-crypto PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)

target_link_libraries(plexus-crypto PUBLIC
    plexus::core
    plexus::wire
    plexus::asio
    plexus::datagram
    plexus_asio_dep
    OpenSSL::SSL
    OpenSSL::Crypto)

target_compile_features(plexus-crypto PUBLIC cxx_std_20)

set_target_properties(plexus-crypto PROPERTIES EXPORT_NAME crypto)

include(GNUInstallDirs)

# OBJECTS DESTINATION makes the imported target carry IMPORTED_OBJECTS: the compiled
# .o files travel to the prefix and an installed consumer folds them into its own link
# unit. Without it CMake silently exports the OBJECT library as a bare INTERFACE and every
# crypto symbol goes undefined at the consumer's link. The objects land beside the
# package, not on the public include/lib path. OpenSSL is resolved at consume time via
# the config package's find_dependency(OpenSSL).
#
# plexus_asio_dep belongs to plexusAsioTargets only (it is shared with the asio backend);
# the crypto link interface reaches it through plexus::asio, so the config package includes
# the asio component's targets file before this one. Exporting it twice trips CMake's
# "some-but-not-all targets in this export set already defined" guard.
install(TARGETS plexus-crypto EXPORT plexusCryptoTargets
    OBJECTS DESTINATION ${CMAKE_INSTALL_LIBDIR}/plexus/objects)
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(EXPORT plexusCryptoTargets
    NAMESPACE plexus::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/plexus
    FILE plexus-crypto-targets.cmake)
