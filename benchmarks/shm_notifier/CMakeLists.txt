# Times the macOS notifiers (the named semaphore and the self-pipe doorbell), whose headers live under the
# shm-include root plexus::asio::shm exports.

if(APPLE AND PLEXUS_ENABLE_SHM_BACKEND)
    add_executable(notifier_wake_latency notifier_wake_latency.cpp)
    target_link_libraries(notifier_wake_latency PRIVATE
        plexus::shm
        plexus::asio
        plexus::asio::shm
        plexus::native)
endif()
