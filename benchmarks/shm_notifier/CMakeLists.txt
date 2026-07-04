# The macOS shm-notifier wake-latency probe. A single-process standalone binary (not a ctest)
# that instantiates ONE templated harness over both notifier variants and times the local
# doorbell delta over an identical named-semaphore carrier. Built only on macOS with the shm
# backend on; the notifier headers live under the shm-include root plexus::asio::shm exports.

if(APPLE AND PLEXUS_ENABLE_SHM_BACKEND)
    add_executable(notifier_wake_latency notifier_wake_latency.cpp)
    target_link_libraries(notifier_wake_latency PRIVATE
        plexus::shm
        plexus::asio
        plexus::asio::shm
        plexus::native)
endif()
