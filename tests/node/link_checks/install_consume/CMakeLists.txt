cmake_minimum_required(VERSION 3.25)
project(plexus_install_consume LANGUAGES CXX)

enable_testing()

# Explicit COMPONENTS so a backend is never implicit: a bare find_package(plexus) resolves only the core and facade.
find_package(plexus CONFIG REQUIRED COMPONENTS ${PLEXUS_CONSUME_COMPONENTS})

# mdnspp is never an installed component, so a non-required query for it must report it absent without erroring.
find_package(plexus CONFIG QUIET COMPONENTS mdnspp)
if(plexus_mdnspp_FOUND)
    message(FATAL_ERROR "plexus_mdnspp_FOUND is TRUE — mdnspp must report absent (no installed component)")
endif()

add_executable(consume main.cpp)
target_link_libraries(consume PRIVATE plexus::plexus)

if("crypto" IN_LIST PLEXUS_CONSUME_COMPONENTS)
    target_link_libraries(consume PRIVATE plexus::crypto)
    target_compile_definitions(consume PRIVATE PLEXUS_CONSUME_CRYPTO)
    # The OBJECT library's compiled .o files must be folded into this link unit.
    target_sources(consume PRIVATE $<TARGET_OBJECTS:plexus::crypto>)
endif()
if("asio" IN_LIST PLEXUS_CONSUME_COMPONENTS)
    target_link_libraries(consume PRIVATE plexus::asio)
endif()
if("native" IN_LIST PLEXUS_CONSUME_COMPONENTS)
    target_link_libraries(consume PRIVATE plexus::native)
endif()

add_test(NAME consume COMMAND consume)
