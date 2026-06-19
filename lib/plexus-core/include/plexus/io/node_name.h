#ifndef HPP_GUARD_PLEXUS_IO_NODE_NAME_H
#define HPP_GUARD_PLEXUS_IO_NODE_NAME_H

#include "plexus/node_id.h"

#include <string>
#include <cstddef>

namespace plexus::io {

// Derive a peer's display/key node_name from its FULL 128-bit node_id. The whole id
// is hex-encoded — never a single byte: the message_forwarder keys its subscribe
// refcounts and its durable-demand ledger by node_name, so two ids that differ in
// any byte MUST map to distinct names or their demand silently collides (a peer
// fires ready without subscribing, or resurrects another peer's topics). "peer-"
// plus 32 lowercase hex chars.
inline std::string node_name_of(const node_id &id)
{
    static constexpr char k_hex[] = "0123456789abcdef";
    std::string           name    = "peer-";
    name.reserve(name.size() + id.size() * 2);
    for(auto b : id)
    {
        const auto v = std::to_integer<unsigned>(b);
        name += k_hex[v >> 4];
        name += k_hex[v & 0xF];
    }
    return name;
}

}

#endif
