#ifndef HPP_GUARD_PLEXUS_IO_MESSAGE_INFO_H
#define HPP_GUARD_PLEXUS_IO_MESSAGE_INFO_H

#include "plexus/publisher_gid.h"

#include <cstdint>
#include <optional>

namespace plexus::io {

struct message_info
{
    std::optional<publisher_gid> source_identity{};
    std::uint64_t publication_sequence{};
    std::uint64_t source_timestamp{};
    std::uint64_t reception_timestamp{};
    bool from_intra_process{};
};

}

#endif
