#ifndef HPP_GUARD_PLEXUS_DETAIL_NODE_SELF_CARRIER_H
#define HPP_GUARD_PLEXUS_DETAIL_NODE_SELF_CARRIER_H

#include "plexus/detail/node_upgrade_wiring.h"

#include "plexus/io/polymorphic_byte_channel.h"

#include <span>
#include <string>
#include <memory>
#include <cstddef>
#include <utility>

namespace plexus::detail {

// Both halves of one node-local shm self-ring: the write half (erased to the engine channel so it
// records as the self-route) and the read half's RAII handle (type-erased to shared_ptr<void> so the
// node holds a single map value type while the receive companion's own dtor clears the sink and
// releases the ring). One node holds both halves of the same join_live ring — no peer.
template<typename EngineChannel>
struct self_carrier_handle
{
    std::unique_ptr<EngineChannel> write;
    std::shared_ptr<void> read;
};

// Mint both halves of a same-node shm self-ring on the ONE join_live ring and bind the read half to
// deliver_cb (which re-enters the node's own dispatch — NOT inject_upgrade_receive, which dead-ends
// for a node with no self-session). Reuses mint_companion/mint_receive_companion as-is; replaces only
// the session-keyed trigger (this is driven by the node's subscribe-gate, not same_host_for) and the
// session-keyed receive routing (deliver_cb, not inject_upgrade_receive). Names no shm type — the
// member is reached generically by the caller.
template<typename EngineChannel, typename Member, typename DeliverCb>
self_carrier_handle<EngineChannel> install_self_carrier(Member &shm_member, const std::string &fqn, DeliverCb deliver_cb)
{
    auto write = wrap_companion<EngineChannel>(shm_member.mint_companion(fqn));
    auto read  = shm_member.mint_receive_companion(fqn, [deliver = std::move(deliver_cb)](std::span<const std::byte> frame) mutable { deliver(frame); });
    return self_carrier_handle<EngineChannel>{std::move(write), std::shared_ptr<void>{std::move(read)}};
}

}

#endif
