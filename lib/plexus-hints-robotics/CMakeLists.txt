# Opt-in robotics hint vocabulary: a header-only neutral concept enum a consumer sets a
# codec's schema_hint from. It holds no schema data, names no vendor, and links nothing —
# an output backend is the only tier that joins a concept to a concrete schema.
add_library(plexus-hints-robotics INTERFACE)
add_library(plexus::hints-robotics ALIAS plexus-hints-robotics)
set_target_properties(plexus-hints-robotics PROPERTIES EXPORT_NAME hints-robotics)

target_include_directories(plexus-hints-robotics INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)

target_compile_features(plexus-hints-robotics INTERFACE cxx_std_20)

include(GNUInstallDirs)
install(TARGETS plexus-hints-robotics EXPORT plexusTargets)
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
