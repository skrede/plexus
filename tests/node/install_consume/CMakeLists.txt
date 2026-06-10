cmake_minimum_required(VERSION 3.25)
project(plexus_install_consume LANGUAGES CXX)

enable_testing()

# The build's enabled backends, forwarded by the driver. Pulled as explicit COMPONENTS so a
# backend is never implicit: a base find_package(plexus) resolves only core + facade.
find_package(plexus CONFIG REQUIRED COMPONENTS ${PLEXUS_CONSUME_COMPONENTS})

# mdnspp is never an installed component (its targets file is intentionally absent); a
# NON-REQUIRED query for it must set plexus_mdnspp_FOUND FALSE without erroring.
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
if("shm" IN_LIST PLEXUS_CONSUME_COMPONENTS)
    target_link_libraries(consume PRIVATE plexus::shm)
endif()

add_test(NAME consume COMMAND consume)
