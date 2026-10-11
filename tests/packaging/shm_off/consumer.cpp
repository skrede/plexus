#include "plexus/asio/node_aliases.h"
#include "plexus/asio/same_host_transports.h"

#include <type_traits>

static_assert(std::is_same_v<plexus::asio::same_host_transports::set_type, plexus::asio::transport_set<plexus::asio::unix_transport, plexus::asio::asio_transport>>,
              "a tree without the shared-memory backend must select the AF_UNIX + TCP same-host composition");
