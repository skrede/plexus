# Inproc-Policy runtime test: posted-fn ordering + drain quiescence, posted-only
# byte delivery, close-notifies-partner, static_discovery resolve, the null_logger
# warn-and-drop seam, and the alloc-counter harness. Links the header-only core,
# the wire layer, and the inproc backend; the `inproc.` ctest prefix lets
# `ctest -R inproc` select it. Auto-discovered by the tests glob.
add_executable(test_inproc_policy test_inproc_policy.cpp)

# tests/ is on the include path so the test can pull in support/alloc_counter.h.
target_include_directories(test_inproc_policy PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/..)

target_link_libraries(test_inproc_policy PRIVATE
    plexus::core
    plexus::wire
    plexus::inproc
    plexus::discovery
    Catch2::Catch2WithMain)

# RUN_SERIAL: the alloc-counter harness reads the single-TU global operator-new counter;
# serializing keeps the count immune to the CPU contention of concurrent test processes.
catch_discover_tests(test_inproc_policy TEST_PREFIX "inproc."
    PROPERTIES RUN_SERIAL TRUE)

# Object-lane runtime test: process-tier object-handle delivery between channels,
# FIFO ordering vs byte packets on one pair, and leak-free refcount release on the
# delivered, closed-partner, and vanished-target paths.
add_executable(test_inproc_object_lane test_inproc_object_lane.cpp)

target_link_libraries(test_inproc_object_lane PRIVATE
    plexus::core
    plexus::wire
    plexus::inproc
    Catch2::Catch2WithMain)

catch_discover_tests(test_inproc_object_lane TEST_PREFIX "inproc.")
