# Only test targets link it. It links plexus::inproc to reuse the deterministic step executor and virtual-clock timer.
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
