# Opt-in mDNS interop example: the living proof the public plexus::discovery
# contract suffices for a real third-party backend (Avahi/Bonjour/dns-sd over
# real mDNS). It is a leaf, off the mandatory core/backends link surface — the
# core never pulls mdnspp or asio through this. Self-gated so the default
# examples build stays mdnspp-free and C++20.
if(NOT PLEXUS_BUILD_MDNS_EXAMPLE)
    return()
endif()

# The demo drives a real asio TCP round-trip alongside the mDNS browse, so it needs
# the asio backend.
if(NOT TARGET plexus::asio)
    message(WARNING "PLEXUS_BUILD_MDNS_EXAMPLE is ON but the asio backend is not — skipping mdns_interop")
    return()
endif()

# plexus_asio_dep carries plexus's own asio resolution (find_package -> pkg-config
# -> FetchContent asio-1-36-0); the adapter TU links it for plexus's own asio use.
include(${PROJECT_SOURCE_DIR}/cmake/plexus-deps.cmake)

# mdnspp v0.3.7 builds asio as an always-present sublibrary (mdnspp::asio) that
# resolves asio only via asio_SOURCE_DIR (its own FetchContent) or find_package
# CONFIG. A pkg-config-only host has neither, so mdnspp::asio would be skipped;
# MDNSPP_CMAKE_FETCH_DEPS ON makes mdnspp fetch the same pinned asio-1-36-0 tag
# plexus pins, so the one copy is byte-identical even though it is fetched twice.
# Setting PLEXUS_MDNSPP_SOURCE_DIR overrides the mdnspp fetch with an on-disk
# checkout (offline / vetted builds).
set(MDNSPP_CMAKE_FETCH_DEPS ON CACHE BOOL "" FORCE)
set(MDNSPP_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(MDNSPP_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)

include(FetchContent)
if(PLEXUS_MDNSPP_SOURCE_DIR)
    FetchContent_Declare(mdnspp SOURCE_DIR ${PLEXUS_MDNSPP_SOURCE_DIR})
else()
    FetchContent_Declare(mdnspp
        GIT_REPOSITORY git@github.com:skrede/mdnspp.git
        GIT_TAG v0.3.7)
endif()
FetchContent_MakeAvailable(mdnspp)

# The adapter TU is C++23 (mdnspp is C++23); the core/wire stay C++20.
add_library(mdns_interop_adapter STATIC src/mdnspp_discovery.cpp)
target_include_directories(mdns_interop_adapter PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(mdns_interop_adapter PUBLIC
    plexus::core
    mdnspp::asio
    plexus_asio_dep)
set_target_properties(mdns_interop_adapter PROPERTIES
    CXX_STANDARD 23
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF)

add_executable(mdns_interop main.cpp)
target_link_libraries(mdns_interop PRIVATE
    mdns_interop_adapter
    plexus::wire
    plexus::asio)
set_target_properties(mdns_interop PROPERTIES
    CXX_STANDARD 23
    CXX_STANDARD_REQUIRED ON
    CXX_EXTENSIONS OFF)
