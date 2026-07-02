#ifndef HPP_GUARD_PLEXUS_ASIO_SHM_WINDOWS_SHM_MEMBER_H
#define HPP_GUARD_PLEXUS_ASIO_SHM_WINDOWS_SHM_MEMBER_H

#include "plexus/asio/shm/windows/event_notifier.h"

#include "plexus/asio/asio_policy.h"

#include "plexus/native/win_shm_region_broker.h"

#include "plexus/shm/shm_mux_member.h"

#include "plexus/muxify.h"

#include <atomic>
#include <string>
#include <cstdint>
#include <optional>
#include <string_view>
#include <type_traits>

namespace plexus::asio::shm {

// The shm multiplexer member over the Windows broker + the named-event reactor bridge: it pulls
// in no tls/dtls/udp/openssl, so a crypto-free composition can compose shared memory.
using shm_member = ::plexus::shm::shm_mux_member<::plexus::native::win_shm_region_broker, event_notifier<muxify<asio_policy>>>;

// Emplaces an event_notifier over (executor, word, park, region_name) once the registry binds
// each ring; the region name derives the named event both peers open.
inline ::plexus::shm::shm_topic_registry<::plexus::native::win_shm_region_broker, event_notifier<muxify<asio_policy>>>::notifier_binder make_bridge_binder(::asio::io_context &io)
{
    return [&io](std::optional<event_notifier<muxify<asio_policy>>> &slot, std::atomic<std::uint32_t> &word, std::atomic<std::uint32_t> &park, std::string_view region_name) { slot.emplace(io, word, park, region_name); };
}

// The broker is borrowed (it must outlive the member); the binder captures `io` so each ring's
// notifier wakes on that reactor. An empty region_ns shares rings by topic (the default); a
// distinct namespace isolates this application's same-host shm from unrelated co-host apps.
inline shm_member make_shm_member(::asio::io_context &io, ::plexus::native::win_shm_region_broker &broker, io::reliability rel = io::reliability::reliable,
                                  io::congestion cong = io::congestion::block, std::string region_ns = {})
{
    return shm_member{broker, rel, cong, make_bridge_binder(io), std::move(region_ns)};
}

}

namespace plexus::native {

// The per-leaf factory transport_set finds by ADL on the broker argument, so transport_set.h can
// build an shm leaf from {io, broker} WITHOUT including this shm-only header.
inline ::plexus::asio::shm::shm_member plexus_make_set_leaf(std::type_identity<::plexus::asio::shm::shm_member>, ::asio::io_context &io, win_shm_region_broker &broker,
                                                            std::string region_ns = {})
{
    return ::plexus::asio::shm::make_shm_member(io, broker, io::reliability::reliable, io::congestion::block, std::move(region_ns));
}

}

#endif
