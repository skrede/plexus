add_executable(plexus_fuzz_seeds plexus_fuzz_seeds.cpp link_seeds.cpp serial_seeds.cpp session_seeds.cpp transport_seeds.cpp wire_frame_seeds.cpp)
target_link_libraries(plexus_fuzz_seeds PRIVATE plexus::datagram plexus::stream plexus::wire plexus::shm plexus::core)
target_include_directories(plexus_fuzz_seeds PRIVATE ${PROJECT_SOURCE_DIR}/tests)
