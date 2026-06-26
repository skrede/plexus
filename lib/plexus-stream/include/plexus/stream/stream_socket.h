#ifndef HPP_GUARD_PLEXUS_STREAM_STREAM_SOCKET_H
#define HPP_GUARD_PLEXUS_STREAM_STREAM_SOCKET_H

#include <span>
#include <cstddef>
#include <concepts>
#include <system_error>

namespace plexus::stream {

// A connected reliable-byte-stream seam: the pure-logic injection point a channel frames
// over, so the channel never names the OS socket calls. The implementors are OS-touching and
// live in a backend (the on-target lwIP socket, the host's POSIX socket); this header stays
// header-free of any OS surface so plexus-stream's "pulls in no OS header" charter holds. Kept
// symmetric in shape with the planned datagram seam: connect/send/recv/close vs bind/
// send_multicast/on_datagram/close.
template<typename S>
concept stream_socket = requires(S &s, typename S::endpoint_type ep, std::span<const std::byte> out, std::span<std::byte> in) {
    typename S::endpoint_type;
    { s.connect(ep) } -> std::same_as<std::error_code>;
    // send/recv are non-blocking and report a byte count, never a negative span length: recv
    // returns 0 when no data is ready under non-block, and signals a hard drop out-of-band (the
    // closed predicate) so the channel's poll() distinguishes "nothing yet" from "connection gone".
    { s.send(out) } -> std::same_as<std::size_t>;
    { s.recv(in) } -> std::same_as<std::size_t>;
    { s.closed() } -> std::same_as<bool>;
    { s.close() } -> std::same_as<void>;
};

}

#endif
