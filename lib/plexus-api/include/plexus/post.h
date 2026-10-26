#ifndef HPP_GUARD_PLEXUS_POST_H
#define HPP_GUARD_PLEXUS_POST_H

#include "plexus/policy.h"

#include "plexus/detail/compat.h"
#include "plexus/detail/affinity_check.h"

#include <utility>
#include <concepts>

namespace plexus {

// The only way into a node from a thread that does not drive its executor: fn runs later on the driver, which alone
// touches node state; executor() is the one node member safe to call from any thread. False means the executor refused
// the work, which then never runs. A callable larger than move_only_function's inline buffer allocates on the posting
// thread.
template<typename Node, typename Fn>
    requires Policy<typename Node::policy_type> && std::invocable<Fn &>
bool post(Node &node, Fn fn)
{
    return Node::policy_type::post_from_thread(node.executor(), plexus::detail::move_only_function<void()>(std::move(fn)));
}

// The driver's last act: it hands the node's executor back, and the next caller binds anew. Only the thread that holds
// the claim may call it.
#if defined(PLEXUS_AFFINITY_CHECKS)
template<typename Node>
void release_driver(Node &node)
{
    plexus::detail::check_affinity<typename Node::policy_type>(node.executor(), "release_driver");
    Node::policy_type::release_driver(node.executor());
}
#else
template<typename Node>
void release_driver(Node &)
{
}
#endif

}

#endif
