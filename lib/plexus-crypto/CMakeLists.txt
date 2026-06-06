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
    src/dtls_channel.cpp)
add_library(plexus::crypto ALIAS plexus-crypto)

# Headers are exposed as INTERFACE include dirs (PUBLIC so the OBJECT TU itself
# compiles against them too); the link deps are carried as INTERFACE so every
# consumer that links plexus::crypto inherits OpenSSL + the asio substrate.
target_include_directories(plexus-crypto PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)

target_link_libraries(plexus-crypto PUBLIC
    plexus::plexus
    plexus::wire
    plexus::asio
    plexus_asio_dep
    OpenSSL::SSL
    OpenSSL::Crypto)

target_compile_features(plexus-crypto PUBLIC cxx_std_20)
