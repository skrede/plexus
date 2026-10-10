#ifndef HPP_GUARD_PLEXUS_ASIO_STREAM_SOCKET_OPTIONS_H
#define HPP_GUARD_PLEXUS_ASIO_STREAM_SOCKET_OPTIONS_H

#include <cstddef>
#include <cstdint>

namespace plexus::asio {

// The kernel socket knobs a stream backend applies once at the open transition. Every field is a
// sentinel: 0 (or false) leaves the kernel default untouched, a non-zero value is a best-effort
// override (a rejection is swallowed — a buffer size is a throughput hint). The granular
// keepalive intervals apply ONLY when keepalive is true AND the interval is non-zero.
struct stream_socket_options
{
    std::size_t so_sndbuf                 = 0;
    std::size_t so_rcvbuf                 = 0;
    bool keepalive                        = false;
    std::uint32_t keepalive_idle_secs     = 0;
    std::uint32_t keepalive_interval_secs = 0;
    std::uint32_t keepalive_count         = 0;
};

// 64 KiB clears one max TLS record (~16 KiB) with margin; a constrained target dials it down via
// the channel's read-buffer ctor argument.
inline constexpr std::size_t k_stream_read_buffer_bytes = 64u * 1024u;

// The fail-closed floor: a degenerate (sub-floor) request floors here rather than producing a
// zero-size buffer (a zero-length async_read makes no progress).
inline constexpr std::size_t k_min_stream_read_buffer_bytes = 4u * 1024u;

constexpr std::size_t stream_read_buffer_size(std::size_t requested) noexcept
{
    return requested < k_min_stream_read_buffer_bytes ? k_min_stream_read_buffer_bytes : requested;
}

}

#endif
