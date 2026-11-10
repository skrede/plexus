# The rigs report numbers and never assert, so they are built to keep them from rotting but are not registered as tests.

add_executable(typed_inproc_rig typed_inproc_rig.cpp)
target_link_libraries(typed_inproc_rig PRIVATE
    plexus::plexus
    plexus::inproc plexus::discovery)
target_include_directories(typed_inproc_rig PRIVATE
    ${PROJECT_SOURCE_DIR}/tests)

add_executable(inproc_upper_layer_rig inproc_upper_layer_rig.cpp)
target_link_libraries(inproc_upper_layer_rig PRIVATE
    plexus::core
    plexus::wire
    plexus::inproc)
target_include_directories(inproc_upper_layer_rig PRIVATE
    ${PROJECT_SOURCE_DIR}/tests)

add_executable(recorder_sweep recorder_sweep.cpp)
target_link_libraries(recorder_sweep PRIVATE
    plexus::plexus
    plexus::inproc plexus::discovery)
target_include_directories(recorder_sweep PRIVATE
    ${PROJECT_SOURCE_DIR}/tests)
