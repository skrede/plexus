# This file and the root CMakeLists.txt are the only two shared build files. A test area registers by adding its own
# tests/<area>/CMakeLists.txt, found by the one-level glob below, and its Catch tests through plexus_add_catch_test; it
# never edits either file. The fuzz subtree is gated apart so a fuzz-only configure needs no Catch2.
if(PLEXUS_BUILD_FUZZ_TESTS)
    add_subdirectory(fuzz)
endif()

if(PLEXUS_BUILD_TESTS)
    # 3.7.1 is the first release whose catch_discover_tests sets SKIP_RETURN_CODE 4, so a binary whose every case
    # skips reports as skipped, not failed.
    find_package(Catch2 3.7.1 CONFIG QUIET)
    if(NOT Catch2_FOUND)
        include(FetchContent)
        FetchContent_Declare(Catch2
            GIT_REPOSITORY https://github.com/catchorg/Catch2.git
            GIT_TAG v3.16.1)
        FetchContent_MakeAvailable(Catch2)
    endif()
    include(Catch)
    include(${PROJECT_SOURCE_DIR}/cmake/plexus-tests.cmake)
    message(STATUS "plexus tests: Catch2 ${Catch2_VERSION} resolved")

    # A directory-scope usage requirement so every area can include the portable test-support header without each
    # target restating it; the target is INTERFACE, so this adds include paths, never a compiled artifact.
    link_libraries(plexus::testing)

    # The leak gate runs as an ALL target in every configured tree: the allowlisted backend dirs are excluded either
    # way, so its result does not depend on PLEXUS_ENABLE_SHM_BACKEND.
    include(${PROJECT_SOURCE_DIR}/cmake/shm_leak_gate.cmake)
    include(${PROJECT_SOURCE_DIR}/cmake/size_gate.cmake)
    # The function-size and cognitive-complexity gate reaches the header-only template bodies through the test TUs
    # that instantiate them, so it needs the compile database those TUs produce.
    include(${PROJECT_SOURCE_DIR}/cmake/function_size_gate.cmake)

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
