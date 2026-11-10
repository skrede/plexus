# Standalone asio, shared with the mDNS interop example. The resolution order mirrors mdnspp's, down to the tag it
# pins, so one asio copy serves both and no two include trees clash under the ODR. The resolved version is reported
# so transport runs are reproducible.

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

# Windows: _WIN32_WINNT pins a modern IOCP/AF_UNIX feature level, NOMINMAX drops the min/max macros that clash with
# the standard library, and WIN32_LEAN_AND_MEAN keeps windows.h from pulling winsock.h in ahead of winsock2.h.
target_compile_definitions(plexus_asio_dep INTERFACE
    ASIO_STANDALONE
    $<$<PLATFORM_ID:Windows>:_WIN32_WINNT=0x0A00>
    $<$<PLATFORM_ID:Windows>:NOMINMAX>
    $<$<PLATFORM_ID:Windows>:WIN32_LEAN_AND_MEAN>)

# ws2_32 is Winsock2; mswsock carries its overlapped and extended entry points.
target_link_libraries(plexus_asio_dep INTERFACE
    $<$<PLATFORM_ID:Windows>:ws2_32>
    $<$<PLATFORM_ID:Windows>:mswsock>)
