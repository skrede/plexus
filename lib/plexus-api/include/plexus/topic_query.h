#ifndef HPP_GUARD_PLEXUS_TOPIC_QUERY_H
#define HPP_GUARD_PLEXUS_TOPIC_QUERY_H

#include "plexus/graph/topic_record.h"
#include "plexus/graph/participant_record.h"

#include "plexus/match/key_pattern.h"

#include "plexus/detail/fail_closed.h"

#include <span>
#include <future>
#include <optional>

namespace plexus {

// The foreign-thread crossing for the executor-affine topics() snapshot: it posts the fill onto the
// node's owning executor and blocks the caller until it completes, so the topic table is only ever
// touched on its owning thread. Called inside a handler of the node's executor it would wait on itself,
// so it is rejected; a single-threaded program that calls it between its own polls still hangs.
// Host-only convenience — the primitive accessor never posts, so the promise/future never enters the
// executor-affine (MCU) read path.
template<typename Node>
graph::snapshot_result topics_blocking(Node &node, std::span<graph::topic_record> out,
                                       const std::optional<match::key_pattern> &filter = std::nullopt)
{
    if(Node::policy_type::inside_executor(node.executor()))
        plexus::detail::fail_closed("plexus: topics_blocking called inside a handler of the node's own executor");
    std::promise<graph::snapshot_result> done;
    std::future<graph::snapshot_result>  ready = done.get_future();
    Node::policy_type::post_from_thread(node.executor(), [&node, out, &filter, &done]() { done.set_value(node.topics(out, filter)); });
    return ready.get();
}

}

#endif
