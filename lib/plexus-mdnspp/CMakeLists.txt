# Gated, compiled mdnspp discovery adapter: the cold-path discovery seam backed
# by real mDNS. The header-only core (plexus, plexus-wire) never links mdnspp or
# asio — this target, and only this target, pulls them in, and only when the
# adapter is ON. Auto-discovered by the root lib glob; self-gated so the core
# stays mdnspp-free when PLEXUS_ENABLE_MDNSPP_DISCOVERY is OFF.
#
# The option is declared here (not in the root CMakeLists.txt) so the target
# registers by adding this fragment alone. The adapter TU compiles at C++23
# per-target (mdnspp is a C++23 library) while the core stays C++20.
option(PLEXUS_ENABLE_MDNSPP_DISCOVERY "Build the mdnspp-backed discovery adapter" OFF)

if(NOT PLEXUS_ENABLE_MDNSPP_DISCOVERY)
    return()
endif()

# Shared, pinned standalone-asio dependency (one copy across plexus + mdnspp).
# mdnspp's own asio resolution mirrors this order (find_package -> pkg-config ->
# FetchContent asio-1-36-0); on a host with a system asio both projects bind the
# same copy, so the adapter that links both plexus::asio and mdnspp::asio sees a
# single asio include tree (no ODR clash).
include(${PROJECT_SOURCE_DIR}/cmake/plexus-deps.cmake)

# Consume mdnspp with the asio policy ON and encryption OFF (no libsodium/OpenSSL
# in the slice). The default fetches the develop branch from GitHub; setting
# PLEXUS_MDNSPP_SOURCE_DIR overrides that with an on-disk source tree (the path a
# local mdnspp checkout lives at), which keeps the version vetted/reproducible
# and lets an offline host build against a working copy.
set(MDNSPP_ENABLE_ASIO_POLICY ON CACHE BOOL "" FORCE)
set(MDNSPP_ENABLE_ENCRYPT OFF CACHE BOOL "" FORCE)
set(MDNSPP_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(MDNSPP_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)

include(FetchContent)
if(PLEXUS_MDNSPP_SOURCE_DIR)
    FetchContent_Declare(mdnspp SOURCE_DIR ${PLEXUS_MDNSPP_SOURCE_DIR})
else()
    FetchContent_Declare(mdnspp
        GIT_REPOSITORY git@github.com:skrede/mdnspp.git
        GIT_TAG develop)
endif()
FetchContent_MakeAvailable(mdnspp)

add_library(plexus-mdnspp STATIC src/mdnspp_discovery.cpp)
add_library(plexus::mdnspp ALIAS plexus-mdnspp)

target_include_directories(plexus-mdnspp PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)

target_link_libraries(plexus-mdnspp PUBLIC
    plexus::core
    mdnspp::asio
    plexus_asio_dep)

# The adapter TU is C++23 (mdnspp is C++23); the core/wire stay C++20.
set_target_properties(plexus-mdnspp PROPERTIES
    CXX_STANDARD 23
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF)

# No install/export rules: this target links FetchContent build-tree mdnspp targets, which
# install(EXPORT) cannot carry from a consumed source tree. An installed-mdnspp component
# becomes possible once mdnspp ships its own CMake package; until then the config package's
# component loop reports plexus_mdnspp_FOUND FALSE.
