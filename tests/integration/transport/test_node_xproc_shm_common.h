#ifndef HPP_GUARD_PLEXUS_TESTS_INTEGRATION_TEST_NODE_XPROC_SHM_COMMON_H
#define HPP_GUARD_PLEXUS_TESTS_INTEGRATION_TEST_NODE_XPROC_SHM_COMMON_H

#include "plexus/asio/same_host_transports.h"

#include "plexus/node_options.h"

#include "plexus/io/host_fingerprint.h"
#include "plexus/io/reconnect_config.h"
#include "plexus/io/same_host_geometry.h"

#include "plexus/discovery/contact_card.h"

#include "plexus/topic_qos.h"

#include <span>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <string>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>

#include <unistd.h>
#include <sys/mman.h>
#include <sys/wait.h>

// Nested directives, not one `defined(__has_feature) && __has_feature(...)` expression: a
// compiler without __has_feature (gcc < 14) replaces the identifier with 0 in a single #if and
// then fails to parse `0(thread_sanitizer)`, so the token is only reached under an outer guard.
#if defined(__SANITIZE_THREAD__)
#define PLEXUS_TEST_UNDER_TSAN 1
#elif defined(__has_feature)
#if __has_feature(thread_sanitizer)
#define PLEXUS_TEST_UNDER_TSAN 1
#endif
#endif

namespace node_xproc_shm_fixture {

namespace pasio = plexus::asio;

#if defined(PLEXUS_TEST_UNDER_TSAN)
inline constexpr bool k_under_tsan = true;
#else
inline constexpr bool k_under_tsan = false;
#endif

inline constexpr std::size_t k_kib = 1024;

// A message under this per-topic cap rides the wire_fallback companion ring; one over it reroutes over the wire.
inline constexpr std::uint32_t k_cap_bytes = 2 * k_kib;

using same_host_node = decltype(std::declval<pasio::same_host_transports &>().make_node(std::declval<plexus::discovery::discovery &>(), std::declval<const plexus::node_id &>(),
                                                                                        std::declval<plexus::node_options>()));

inline plexus::node_id make_id(std::uint8_t seed)
{
    plexus::node_id id{};
    id[0] = std::byte{seed};
    return id;
}

inline plexus::node_options dial_opts(bool eager)
{
    plexus::node_options opts;
    opts.reconnect    = plexus::io::reconnect_config{std::chrono::milliseconds(20), std::chrono::milliseconds(500), std::nullopt, std::nullopt};
    opts.redial_seed  = 0xC0FFEEu;
    opts.dial_eagerly = eager;
    return opts;
}

// Distinct fingerprints fail is_same_host, so the pair never reaches shared memory and stays on the wire.
inline plexus::node_options distinct_fingerprint_opts(bool eager, std::uint64_t fp)
{
    plexus::node_options opts        = dial_opts(eager);
    opts.handshake.local_fingerprint = plexus::io::host_fingerprint{fp};
    return opts;
}

inline plexus::discovery::service_info peer_card(const std::string &name, const plexus::node_id &id, std::uint16_t tcp_port)
{
    return {name, plexus::io::endpoint{"", "127.0.0.1"}, plexus::discovery::assemble_contact_card(id, {{"tcp", tcp_port}})};
}

inline std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

struct coord
{
    std::atomic<std::uint32_t> ring_ready{0}; // the publisher minted the companion ring
    std::atomic<std::uint32_t> shm_seen{0};   // the raw consumer saw the fitting payload on the ring
    std::atomic<std::uint32_t> consumer_done{0};
};

inline coord *map_coord()
{
    void *p = ::mmap(nullptr, sizeof(coord), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    return p == MAP_FAILED ? nullptr : ::new(p) coord{};
}

inline int reap(pid_t pid)
{
    int status = 0;
    while(::waitpid(pid, &status, 0) < 0)
        if(errno != EINTR)
            return -1;
    return status;
}

// The same-host hint makes the upgrade eligible; the small cap makes the companion the wire_fallback bounded ring.
inline plexus::topic_qos companion_qos()
{
    plexus::topic_qos qos;
    qos.dispatch          = plexus::io::dispatch_hint::frequent;
    qos.max_message_bytes = k_cap_bytes;
    return qos;
}

inline plexus::io::same_host_geometry companion_geometry()
{
    return plexus::io::same_host_geometry{2u, plexus::io::ring_geometry_mode::wire_fallback};
}

struct medium_result
{
    bool small_at_callback = false; // the subscriber's user callback received the fitting payload
    bool large_at_callback = false; // the subscriber's user callback received the over-cap payload
    bool small_over_shm    = false; // the raw consumer saw the fitting payload on the SHM ring
    bool region_observed   = false;
};

// A fitting payload may ride the wire until the lane engages, so it counts only after the child's 'E' marker (its raw
// observer saw a ring frame). Under TSan, whose slowdown makes that observer unreliable, any arrival counts: it proves
// delivery, not the medium. Returns how many over-cap payloads arrived.
inline std::uint32_t take_tags(int fd, medium_result &result, bool &engaged)
{
    char tag               = 0;
    std::uint32_t over_cap = 0;
    while(::read(fd, &tag, 1) == 1)
    {
        engaged                  = engaged || tag == 'E' || k_under_tsan;
        result.small_at_callback = result.small_at_callback || (tag == 'S' && engaged);
        result.large_at_callback = result.large_at_callback || tag == 'L';
        over_cap += tag == 'L' ? 1u : 0u;
    }
    return over_cap;
}

}

#endif
