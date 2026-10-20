# Header-only reusable test substrate: the consolidated virtual clock, a pure
# recording mock byte_channel + the Policy-satisfying mock_policy, and the harness
# fixture (virtual-clock advance + drain to quiescence). Reuses the deterministic
# inproc step-executor and virtual-clock timer rather than reinventing them, so it
# links plexus::inproc alongside the core. Auto-discovered by the root lib glob;
# an INTERFACE target carries no compiled artifact, so only test targets ever link it.
add_library(plexus-testing INTERFACE)
add_library(plexus::testing ALIAS plexus-testing)

target_include_directories(plexus-testing INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)

target_link_libraries(plexus-testing INTERFACE plexus::core plexus::wire plexus::inproc)

# platform.h includes <windows.h>, so a test that does not also link the asio dependency needs the same Windows
# definitions it carries; without NOMINMAX the min/max function-like macros break std::min and time_point::max.
target_compile_definitions(plexus-testing INTERFACE
    $<$<PLATFORM_ID:Windows>:NOMINMAX>
    $<$<PLATFORM_ID:Windows>:WIN32_LEAN_AND_MEAN>)

target_compile_features(plexus-testing INTERFACE cxx_std_20)
