#include "plexus/asio/shm/linux/ring_notifier.h"

#include "plexus/asio/asio_policy.h"

#include <type_traits>

static_assert(std::is_same_v<plexus::asio::shm::ring_notifier<plexus::asio::asio_policy>, plexus::asio::shm::ring_notifier_threaded<plexus::asio::asio_policy>>,
              "a liburing without io_uring_prep_futex_wait must select the threaded notifier floor");
