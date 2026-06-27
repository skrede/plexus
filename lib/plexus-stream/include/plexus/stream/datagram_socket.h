#ifndef HPP_GUARD_PLEXUS_STREAM_DATAGRAM_SOCKET_H
#define HPP_GUARD_PLEXUS_STREAM_DATAGRAM_SOCKET_H

#include "plexus/detail/compat.h"

#include <span>
#include <cstddef>
#include <concepts>
#include <system_error>

namespace plexus::stream {

// A connectionless multicast-datagram seam: the pure-logic injection point discovery announces
// over, so it never names the OS socket calls. The implementors are OS-touching and live in a
// backend (the host's asio multicast socket, the on-target lwIP socket); this header stays
// header-free of any OS surface so plexus-stream's "pulls in no OS header" charter holds. Kept
// symmetric in shape with the stream seam: bind/send_multicast/on_datagram/close vs connect/
// send/recv/close. on_datagram carries the kernel source endpoint (the unspoofable sender).
template<typename S>
concept datagram_socket = requires(S &s, typename S::endpoint_type ep, std::span<const std::byte> out,
                                   plexus::detail::move_only_function<void(const typename S::endpoint_type &, std::span<const std::byte>)> fn) {
    typename S::endpoint_type;
    { s.bind(ep) } -> std::same_as<std::error_code>;
    { s.send_multicast(out) } -> std::same_as<void>;
    { s.on_datagram(std::move(fn)) } -> std::same_as<void>;
    { s.close() } -> std::same_as<void>;
};

}

#endif
