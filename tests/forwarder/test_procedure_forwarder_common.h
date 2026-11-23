#ifndef HPP_GUARD_PLEXUS_TESTS_FORWARDER_TEST_PROCEDURE_FORWARDER_COMMON_H
#define HPP_GUARD_PLEXUS_TESTS_FORWARDER_TEST_PROCEDURE_FORWARDER_COMMON_H

#include "plexus/io/frame_router.h"
#include "plexus/io/wire_forwarder.h"
#include "plexus/io/procedure_forwarder.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"

#include "plexus/log/logger.h"

#include "plexus/policy.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/rpc_frame.h"
#include "plexus/wire/subscribe.h"
#include "plexus/wire/data_frame.h"
#include "plexus/wire/rpc_status.h"
#include "plexus/wire/topic_hash.h"
#include "plexus/wire/frame_codec.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <chrono>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>
#include <string_view>
#include <system_error>

namespace procedure_forwarder_fixture {

using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_channel;
using procedure_forwarder = plexus::io::procedure_forwarder<inproc_policy>;
using plexus::wire::rpc_status;

// The maintainability gate: the procedure forwarder models the wire_forwarder
// shape over its own peer, exactly as message_forwarder does.
static_assert(plexus::io::wire_forwarder<procedure_forwarder, procedure_forwarder::peer>);

// A deadline far past any clock advance the non-timeout cases perform — the inproc
// clock only moves when a test explicitly advances it, so a roundtrip/orphan case
// never trips this. The dedicated timeout cases pass their own short deadline.
constexpr auto k_long_deadline = std::chrono::hours(1);

inline std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

inline std::string to_string(std::span<const std::byte> b)
{
    return std::string{reinterpret_cast<const char *>(b.data()), b.size()};
}

// A counting logger: warn() bumps a counter, proving the warn-and-drop seam fires.
struct counting_logger final : plexus::log::logger
{
    counting_logger()
            : count(0)
    {
    }

    void warn(std::string_view) override
    {
        ++count;
    }

    std::size_t count;
};

// A capture sink recording every frame the forwarder sends.
struct capture
{
    explicit capture(inproc_executor<> &ex)
            : sink(ex)
    {
        sink.on_data([this](std::span<const std::byte> d) { frames.emplace_back(d.begin(), d.end()); });
    }

    inproc_channel<> sink;
    std::vector<std::vector<std::byte>> frames;
};

inline std::size_t count_subscribes(const capture &cap)
{
    std::size_t n = 0;
    for(const auto &f : cap.frames)
    {
        auto hdr = plexus::wire::decode_header(f);
        if(!hdr || hdr->type != plexus::wire::msg_type::subscribe)
            continue;
        auto inner = std::span<const std::byte>{f}.subspan(plexus::wire::header_size);
        if(plexus::wire::decode_subscribe_request(inner))
            ++n;
    }
    return n;
}

inline procedure_forwarder::peer make_peer(inproc_channel<> &tx, capture &cap, std::string node_name)
{
    tx.connect_to(cap.sink.local_endpoint());
    return procedure_forwarder::peer{tx, std::move(node_name)};
}

// Synthesize a header-off rpc_response inner payload for an arbitrary corr_id (the
// orphan probe feeds this straight to deliver_response, bypassing the link).
inline std::vector<std::byte> make_response_inner(std::uint64_t corr_id, rpc_status status, std::span<const std::byte> ret)
{
    plexus::wire::bidirectional_header hdr{
            .source = plexus::wire::endpoint_source_type::procedure, .sequence = 0, .topic_hash = 0, .type_hash_1 = 0, .type_hash_2 = 0, .correlation_id = corr_id};
    std::vector<std::byte> out;
    plexus::wire::encode_rpc_response_into(out, hdr, status, ret);
    return out;
}

}

#endif
