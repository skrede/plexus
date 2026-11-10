# Reads the calling directory's PLEXUS_TEST_PREFIX and PLEXUS_TEST_INCLUDES, which add_subdirectory hands down to
# nested test areas.
function(plexus_add_catch_test name)
    cmake_parse_arguments(PARSE_ARGV 1 arg "NO_MAIN" "PREFIX" "SOURCES;LINKS;INCLUDES;DEFINES;PROPERTIES")
    if(NOT DEFINED arg_PREFIX)
        set(arg_PREFIX "${PLEXUS_TEST_PREFIX}")
    endif()
    if(arg_UNPARSED_ARGUMENTS OR NOT arg_SOURCES OR "${arg_PREFIX}" STREQUAL "")
        message(FATAL_ERROR "plexus_add_catch_test(${name}) needs SOURCES and a PREFIX or PLEXUS_TEST_PREFIX; "
            "unparsed: '${arg_UNPARSED_ARGUMENTS}'")
    endif()
    set(catch Catch2::Catch2WithMain)
    if(arg_NO_MAIN)
        set(catch Catch2::Catch2)
    endif()
    add_executable(${name} ${arg_SOURCES})
    target_include_directories(${name} PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR} ${PLEXUS_TEST_INCLUDES} ${PROJECT_SOURCE_DIR}/tests ${arg_INCLUDES})
    if(arg_DEFINES)
        target_compile_definitions(${name} PRIVATE ${arg_DEFINES})
    endif()
    target_link_libraries(${name} PRIVATE ${arg_LINKS} ${catch})
    catch_discover_tests(${name} TEST_PREFIX "${arg_PREFIX}" PROPERTIES ${arg_PROPERTIES})
endfunction()
