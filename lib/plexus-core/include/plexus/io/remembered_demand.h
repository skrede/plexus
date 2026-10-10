#ifndef HPP_GUARD_PLEXUS_IO_REMEMBERED_DEMAND_H
#define HPP_GUARD_PLEXUS_IO_REMEMBERED_DEMAND_H

#include "plexus/io/subscriber_qos.h"

#include <string>
#include <cstdint>
#include <optional>

namespace plexus::io {

struct remembered_demand
{
    std::string fqn;
    subscriber_qos qos;
    std::optional<std::uint64_t> type_id; // nullopt = undeclared
    // Owned, not borrowed: a replay on reconnect outlives whatever named the type at attach.
    std::string type_name;
};

}

#endif
