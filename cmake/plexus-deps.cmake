# Standalone (non-Boost) asio resolution, also shared by the opt-in mDNS interop example.
#
# Resolving order mirrors mdnspp's so a single asio copy is shared on this host
# (no two include trees => no ODR clash across plexus and the mDNS interop example):
#   1. find_package(asio CONFIG)  — an installed/vcpkg asio::asio target.
#   2. pkg-config asio            — a system/pkg-config install.
#   3. FetchContent asio-1-36-0   — the exact tag mdnspp pins, as a last resort.
# The resolved version is reported so transport runs are reproducible.
#
# Defines the INTERFACE target plexus_asio_dep carrying the asio include path and
# ASIO_STANDALONE (the standalone, header-only, non-Boost configuration).

if(TARGET plexus_asio_dep)
    return()
endif()

add_library(plexus_asio_dep INTERFACE)

find_package(asio CONFIG QUIET)
if(asio_FOUND AND TARGET asio::asio)
    target_link_libraries(plexus_asio_dep INTERFACE asio::asio)
    message(STATUS "plexus: asio resolved via find_package (asio::asio)")
else()
    find_package(PkgConfig QUIET)
    if(PkgConfig_FOUND)
        pkg_check_modules(plexus_asio_pc QUIET IMPORTED_TARGET asio)
    endif()
    if(TARGET PkgConfig::plexus_asio_pc)
        target_link_libraries(plexus_asio_dep INTERFACE PkgConfig::plexus_asio_pc)
        message(STATUS "plexus: asio resolved via pkg-config (${plexus_asio_pc_VERSION})")
    else()
        include(FetchContent)
        FetchContent_Declare(asio
            GIT_REPOSITORY https://github.com/chriskohlhoff/asio.git
            GIT_TAG asio-1-36-0)
        FetchContent_MakeAvailable(asio)
        target_include_directories(plexus_asio_dep SYSTEM INTERFACE
            $<BUILD_INTERFACE:${asio_SOURCE_DIR}/asio/include>)
        message(STATUS "plexus: asio fetched at asio-1-36-0 (mdnspp pin)")
    endif()
endif()

# Standalone, non-Boost asio. _WIN32_WINNT is set on Windows so asio selects a
# supported IOCP feature level (matches mdnspp).
target_compile_definitions(plexus_asio_dep INTERFACE
    ASIO_STANDALONE
    $<$<PLATFORM_ID:Windows>:_WIN32_WINNT=0x0601>)
