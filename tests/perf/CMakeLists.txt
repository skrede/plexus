# Standalone performance rigs: long steady-loop binaries that REPORT numbers (latency,
# throughput, steady-loop heap allocations), never assert. They are built so they cannot
# rot, but are NOT registered as ctests (no catch_discover_tests / add_test) — the parity
# suite count is untouched. tests/ is on the include path for support/alloc_counter.h.

add_executable(typed_inproc_rig typed_inproc_rig.cpp)
target_link_libraries(typed_inproc_rig PRIVATE
    plexus::plexus
    plexus::inproc)
target_include_directories(typed_inproc_rig PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..)

add_executable(inproc_upper_layer_rig inproc_upper_layer_rig.cpp)
target_link_libraries(inproc_upper_layer_rig PRIVATE
    plexus::core
    plexus::wire
    plexus::inproc)
target_include_directories(inproc_upper_layer_rig PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..)

add_executable(recorder_sweep recorder_sweep.cpp)
target_link_libraries(recorder_sweep PRIVATE
    plexus::plexus
    plexus::inproc)
target_include_directories(recorder_sweep PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..)
