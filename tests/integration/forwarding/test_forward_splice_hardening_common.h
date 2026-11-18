#ifndef HPP_GUARD_TESTS_INTEGRATION_FORWARD_SPLICE_HARDENING_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_FORWARD_SPLICE_HARDENING_COMMON_H

#include "plexus/io/message_forwarder.h"
#include "plexus/io/detail/forward_splice.h"
#include "plexus/io/detail/drop_event.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"

#include "plexus/io/null_logger.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/topic_hash.h"

#include "plexus/node_id.h"

#include <span>
#include <string>
#include <vector>
#include <memory>
#include <cstddef>
#include <cstdint>

namespace forward_splice_hardening_fixture {

using namespace plexus;
namespace wire = plexus::wire;

using forwarder  = io::message_forwarder<inproc::inproc_policy>;
using fwd_splice = io::forward_splice<inproc::inproc_policy>;

inline node_id id_of(std::uint8_t base)
{
    node_id id{};
    for(std::size_t i = 0; i < id.size(); ++i)
        id[i] = static_cast<std::byte>(base + i);
    return id;
}

inline io::forward_ctx ctx_with(std::size_t slots, std::size_t slot_bytes)
{
    io::forward_options opts;
    opts.splice_pool_slots = slots;
    opts.splice_slot_bytes = slot_bytes;
    return io::make_forward_ctx(opts);
}

// A header-on unidirectional frame for a topic captured off a producer's wire — the exact shape a relay
// splices inside a forwarded envelope; body_size scales the encoded envelope past a slot when needed.
inline std::vector<std::byte> capture_inner(std::string_view fqn, std::size_t body_size)
{
    inproc::inproc_bus<> bus;
    inproc::inproc_executor<> ex(bus);
    inproc::inproc_channel<> sub(ex);
    inproc::inproc_channel<> cap(ex);
    sub.connect_to(cap.local_endpoint());
    std::vector<std::byte> framed;
    cap.on_data([&](std::span<const std::byte> f) { framed.assign(f.begin(), f.end()); });

    log::null_logger sink;
    forwarder producer{sink};
    producer.declare(fqn, topic_qos{});
    producer.attach_for_fanout(forwarder::peer{sub, "producer"}, fqn);
    ex.drain();
    producer.publish(fqn, std::vector<std::byte>(body_size, std::byte{0xA5}));
    ex.drain();
    return framed;
}

// One downstream leg: a tx the relay fans onto and a rx sink capturing whatever crossed.
struct leg
{
    explicit leg(inproc::inproc_executor<> &ex)
            : tx(ex)
            , rx(ex)
    {
        tx.connect_to(rx.local_endpoint());
        rx.on_data([this](std::span<const std::byte> f) { got.emplace_back(f.begin(), f.end()); });
    }
    inproc::inproc_channel<> tx;
    inproc::inproc_channel<> rx;
    std::vector<std::vector<std::byte>> got;
};

struct relay_fixture
{
    inproc::inproc_bus<> bus;
    inproc::inproc_executor<> ex{bus};
    log::null_logger log_sink;
    forwarder fwd{log_sink};
    std::vector<io::detail::drop_cause> drops;

    relay_fixture()
    {
        fwd.on_drop([this](const io::detail::drop_event &e) { drops.push_back(e.cause); });
    }

    std::size_t drops_of(io::detail::drop_cause cause) const
    {
        std::size_t n = 0;
        for(auto c : drops)
            n += (c == cause);
        return n;
    }
};

}

#endif
