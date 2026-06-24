#ifndef HPP_GUARD_PLEXUS_IO_NODE_NAME_H
#define HPP_GUARD_PLEXUS_IO_NODE_NAME_H

#include "plexus/node_id.h"

#include <string>
#include <cstddef>

namespace plexus::io {

// The FULL id is hex-encoded: the message_forwarder keys its refcounts and demand
// ledger by node_name, so two ids differing in any byte MUST map to distinct names
// or their demand silently collides.
inline std::string node_name_of(const node_id &id)
{
    static constexpr char k_hex[] = "0123456789abcdef";
    std::string name              = "peer-";
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
