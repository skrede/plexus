# The relayed-vs-direct forwarding cost probe. Single process, inproc transports only; links the
# routing engine and the in-process backend directly because the relay wiring composes forward_splice
# onto routing_engine.

add_executable(plexus_relay_forward plexus_relay_forward.cpp)
target_include_directories(plexus_relay_forward PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(plexus_relay_forward PRIVATE plexus::core plexus::inproc)
