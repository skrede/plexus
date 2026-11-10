# A leaf off the core and backend link surface: the core never pulls mdnspp or asio through it. It gates itself, so the
# default examples build stays mdnspp-free and C++20.
if(NOT PLEXUS_BUILD_MDNS_EXAMPLE)
    return()
endif()

# The demo drives an asio TCP round trip beside the mDNS browse.
if(NOT TARGET plexus::asio)
    message(WARNING "PLEXUS_BUILD_MDNS_EXAMPLE is ON but the asio backend is not — skipping mdns_interop")
    return()
endif()

# Provides plexus_asio_dep, plexus's own asio resolution (find_package, then pkg-config, then FetchContent asio-1-36-0),
# which the adapter links for its own asio use.
include(${PROJECT_SOURCE_DIR}/cmake/plexus-deps.cmake)

# mdnspp v0.3.7 resolves asio for mdnspp::asio only through its own FetchContent or find_package CONFIG, so on a
# pkg-config-only host mdnspp::asio would be skipped. MDNSPP_CMAKE_FETCH_DEPS makes mdnspp fetch the asio-1-36-0 tag
# plexus pins, so both copies are byte-identical. PLEXUS_MDNSPP_SOURCE_DIR replaces the mdnspp fetch with an on-disk
# checkout for offline or vetted builds.
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

# The adapter is C++23 because mdnspp is; the core and wire stay C++20.
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
