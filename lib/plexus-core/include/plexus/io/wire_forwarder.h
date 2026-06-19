#ifndef HPP_GUARD_PLEXUS_IO_WIRE_FORWARDER_H
#define HPP_GUARD_PLEXUS_IO_WIRE_FORWARDER_H

#include <concepts>
#include <string_view>

namespace plexus::io {

// The maintainability check: every forwarder a router owns models this shape.
// A plexus "peer" is the byte_channel a frame is fanned over plus the node-name
// key the subscription is rooted at — not a session base — so the concept is
// stated over a deduced Peer the forwarder is parameterized on, with no
// forward-declared session type.
template<typename F, typename Peer>
concept wire_forwarder = requires(F &f, const Peer &peer, std::string_view fqn) {
    { f.attach(peer, fqn) } -> std::convertible_to<bool>;
    { f.detach_all(peer) } -> std::same_as<void>;
};

}

#endif
