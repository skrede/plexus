#ifndef HPP_GUARD_PLEXUS_PARTICIPANT_QUERY_H
#define HPP_GUARD_PLEXUS_PARTICIPANT_QUERY_H

#include "plexus/graph/participant_record.h"

#include <span>
#include <future>

namespace plexus {

// The foreign-thread crossing for the executor-affine participants() snapshot: it posts the fill onto
// the node's owning executor and blocks the caller until it completes, so the awareness table is only
// ever touched on its owning thread. Host-only convenience — the primitive accessor never posts, so the
// promise/future never enters the executor-affine (MCU) read path.
template<typename Node>
graph::snapshot_result participants_blocking(Node &node, std::span<graph::participant_record> out)
{
    std::promise<graph::snapshot_result> done;
    std::future<graph::snapshot_result>  ready = done.get_future();
    Node::policy_type::post(node.executor(), [&node, out, &done]() { done.set_value(node.participants(out)); });
    return ready.get();
}

}

#endif
