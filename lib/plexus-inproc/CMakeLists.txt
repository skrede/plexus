# Header-only deterministic in-process backend: the step-executor, bus,
# virtual-clock timer, byte_channel, and the Policy-satisfying inproc_policy.
# A second, backend-independent substrate for the Policy seam, with zero
# dependencies. Auto-discovered by the root lib glob.
add_library(plexus-inproc INTERFACE)
add_library(plexus::inproc ALIAS plexus-inproc)

target_include_directories(plexus-inproc INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)

target_link_libraries(plexus-inproc INTERFACE plexus::core)

target_compile_features(plexus-inproc INTERFACE cxx_std_20)
