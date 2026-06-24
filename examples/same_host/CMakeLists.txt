# Same-host nodes composing shm + AF_UNIX + plain TCP. Linux-only (the shm bridge headers live
# under shm/linux/) and needs the shm backend target plexus::asio::shm (present only when
# PLEXUS_ENABLE_SHM_BACKEND is on); plexus::asio::shm carries liburing as a usage requirement,
# so the examples link no liburing of their own. Crypto-free: no TLS/DTLS/UDP is pulled in, so
# they build with the crypto backend disabled.
if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND TARGET plexus::asio::shm)
    plexus_example(shm_local_node plexus::plexus plexus::asio plexus::asio::shm plexus::native)
    plexus_example(same_host_node plexus::plexus plexus::asio plexus::asio::shm plexus::native)
endif()
