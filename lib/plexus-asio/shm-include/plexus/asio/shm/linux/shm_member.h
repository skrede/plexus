#ifndef HPP_GUARD_PLEXUS_ASIO_SHM_LINUX_SHM_MEMBER_H
#define HPP_GUARD_PLEXUS_ASIO_SHM_LINUX_SHM_MEMBER_H

#include "plexus/asio/shm/linux/ring_notifier.h"

#include "plexus/asio/asio_policy.h"

#include "plexus/shm/posix_shm_region_broker.h"

#include "plexus/io/shm/shm_mux_member.h"

#include "plexus/muxify.h"

#include <atomic>
#include <cstdint>
#include <optional>
#include <type_traits>

namespace plexus::asio::shm {

// The shm multiplexer member built over the POSIX broker + the io_uring-futex reactor
// bridge (ring_notifier), the only asio-coupled shared-memory piece. This header carries
// the member type and its construction recipe ALONE — it pulls in no tls/dtls/udp/openssl,
// so a lean (crypto-free) composition can compose shared memory without dragging in the
// secure or datagram backends. The all-backends and the lean compositions both include it.
using shm_member = io::shm::shm_mux_member<::plexus::shm::posix_shm_region_broker,
                                           ring_notifier<muxify<asio_policy>>>;

// The notifier-binder for the bridge: capture the io_context and emplace a ring_notifier
// over (executor, word) once the registry binds each ring. Free function so the lambda
// shape lives in one place.
[[nodiscard]] inline io::shm::shm_topic_registry<::plexus::shm::posix_shm_region_broker,
                                                 ring_notifier<muxify<asio_policy>>>::notifier_binder
make_bridge_binder(::asio::io_context &io)
{
    return [&io](std::optional<ring_notifier<muxify<asio_policy>>> &slot, std::atomic<std::uint32_t> &word,
                 std::atomic<std::uint32_t> &park) {
        slot.emplace(io, word, park);
    };
}

// Construct the shm member over the POSIX broker + the reactor bridge bound to `io`. The
// broker is borrowed (it must outlive the member); the binder captures `io` so each ring's
// notifier wakes on that reactor. The member is then composed as the first local candidate.
[[nodiscard]] inline shm_member make_shm_member(::asio::io_context &io,
                                                ::plexus::shm::posix_shm_region_broker &broker,
                                                io::reliability rel = io::reliability::reliable,
                                                io::congestion cong = io::congestion::block)
{
    return shm_member{broker, rel, cong, make_bridge_binder(io)};
}

}

namespace plexus::shm {

// The per-leaf factory transport_set finds by ADL on the broker argument: it lets the
// shm-agnostic transport_set.h build an shm leaf from {io, broker} WITHOUT including this
// shm-only header. A non-shm transport_set never names it, so it pulls in no shm/crypto.
[[nodiscard]] inline ::plexus::asio::shm::shm_member
plexus_make_set_leaf(std::type_identity<::plexus::asio::shm::shm_member>,
                     ::asio::io_context &io, posix_shm_region_broker &broker)
{
    return ::plexus::asio::shm::make_shm_member(io, broker);
}

}

#endif
