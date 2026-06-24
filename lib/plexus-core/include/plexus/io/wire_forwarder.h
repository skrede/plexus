#ifndef HPP_GUARD_PLEXUS_IO_WIRE_FORWARDER_H
#define HPP_GUARD_PLEXUS_IO_WIRE_FORWARDER_H

#include <concepts>
#include <string_view>

namespace plexus::io {

template<typename F, typename Peer>
concept wire_forwarder = requires(F &f, const Peer &peer, std::string_view fqn) {
    { f.attach(peer, fqn) } -> std::convertible_to<bool>;
    { f.detach_all(peer) } -> std::same_as<void>;
};

}

#endif
