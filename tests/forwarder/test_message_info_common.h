// message_info subscriber-callback delivery.
//
// The forwarder's 2-arg deliver path hands the subscriber only (fqn, bytes). The
// opt-in 3-arg overload ALSO receives a message_info carrying {source_identity,
// publication_sequence, source_timestamp, reception_timestamp, from_intra_process}.
// The session assembles the header-derived metadata at on_receive (where the decoded
// frame_header is still live, before the router strips it) and the forwarder fills the
// publication_sequence it alone decodes from the inner payload. from_intra_process is
// derived honestly from the delivering channel's locality tier. source_identity stays
// absent until the gid rides the wire.
#pragma once

#include "plexus/io/message_forwarder.h"
#include "plexus/io/message_info.h"
#include "plexus/io/locality.h"
#include "plexus/io/endpoint.h"

#include "plexus/publisher_gid.h"
#include "plexus/topic_qos.h"
#include "plexus/node_id.h"

#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_bus.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/data_frame.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/topic_hash.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <vector>
#include <string_view>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace message_info_fixture {

using plexus::io::locality;
using plexus::io::tier_of;
using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_channel;
using plexus::inproc::inproc_executor;
using plexus::io::message_info;
using plexus::node_id;
using plexus::publisher_gid;
using forwarder = plexus::io::message_forwarder<inproc_policy>;

inline std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

// A distinct, non-zero peer node_id — the gid's node_id half on reconstruction.
inline node_id peer_node_id(std::uint8_t tail = 0xAB)
{
    node_id id{};
    id[15] = std::byte{tail};
    return id;
}

inline forwarder::peer make_peer(inproc_channel<> &ch, std::string node_name)
{
    return forwarder::peer{ch, std::move(node_name)};
}

} // namespace message_info_fixture
