# Consumable facade target (the name::name library). Re-exports the minimal
# plexus::core engine and carries the facade-only public headers.
add_library(plexus-api INTERFACE)
add_library(plexus::plexus ALIAS plexus-api)
set_target_properties(plexus-api PROPERTIES EXPORT_NAME plexus)

target_include_directories(plexus-api INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)

target_link_libraries(plexus-api INTERFACE plexus::core)

target_compile_features(plexus-api INTERFACE cxx_std_20)

install(TARGETS plexus-api EXPORT plexusTargets)
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
