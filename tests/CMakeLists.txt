# Test-tree registration contract. This file and the root CMakeLists.txt are
# the only two shared build files; every test area registers by adding its own
# tests/<area>/CMakeLists.txt fragment (auto-discovered by the glob below) and
# never edits these two. The fuzz subtree is gated independently of the unit
# suite so a pure-fuzz configure does not need Catch2.

# Descend into the fuzz subtree whenever PLEXUS_BUILD_FUZZ_TESTS is ON,
# independent of PLEXUS_BUILD_TESTS — the harnesses are pure header-only
# decoder drivers and need no Catch2.
if(PLEXUS_BUILD_FUZZ_TESTS)
    add_subdirectory(fuzz)
endif()

if(PLEXUS_BUILD_TESTS)
    # Catch2 v3: prefer an installed package; fall back to FetchContent. The
    # resolved version is reported so the parity-oracle run is reproducible.
    find_package(Catch2 3 CONFIG QUIET)
    if(NOT Catch2_FOUND)
        include(FetchContent)
        FetchContent_Declare(Catch2
            GIT_REPOSITORY https://github.com/catchorg/Catch2.git
            GIT_TAG v3.5.2)
        FetchContent_MakeAvailable(Catch2)
    endif()
    include(Catch)
    message(STATUS "plexus tests: Catch2 ${Catch2_VERSION} resolved")

    # Auto-discover each per-area unit-test fragment (excluding fuzz, gated
    # above). New test areas register by adding their own fragment.
    file(GLOB PLEXUS_TEST_FRAGMENTS CONFIGURE_DEPENDS
        ${CMAKE_CURRENT_SOURCE_DIR}/*/CMakeLists.txt)
    foreach(fragment ${PLEXUS_TEST_FRAGMENTS})
        get_filename_component(fragment_dir ${fragment} DIRECTORY)
        get_filename_component(fragment_name ${fragment_dir} NAME)
        if(NOT fragment_name STREQUAL "fuzz")
            add_subdirectory(${fragment_dir})
        endif()
    endforeach()
endif()
