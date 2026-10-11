#include "plexus/asio/same_host_transports.h"

#include <type_traits>

static_assert(std::is_same_v<plexus::asio::same_host_transports::set_type, plexus::asio::transport_set<plexus::asio::shm::shm_member, plexus::asio::unix_transport, plexus::asio::asio_transport>>,
              "an installed consumer linking the shared-memory target must select the shm-bearing same-host composition");
