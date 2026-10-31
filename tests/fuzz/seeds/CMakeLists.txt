add_executable(plexus_fuzz_seeds plexus_fuzz_seeds.cpp serial_seeds.cpp)
target_link_libraries(plexus_fuzz_seeds PRIVATE plexus::stream plexus::wire)
target_include_directories(plexus_fuzz_seeds PRIVATE ${PROJECT_SOURCE_DIR}/tests)
