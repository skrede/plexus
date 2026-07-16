// Through-the-facade reachability: a single-transport bounded<> node resolves its engine's peer and
// topic tables to the fixed twins the profile names. The direct-instantiation proof in inv1_graph_tu
// augments this — it shows the twin compiles on any platform; this shows the facade reaches it.

#include "plexus/node.h"

#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/io/fixed_peer_storage.h"

#include "plexus/graph/fixed_topic_storage.h"

#include <type_traits>

namespace {

using policy    = plexus::inproc::inproc_policy;
using transport = plexus::inproc::inproc_transport<>;

using bounded_node = plexus::node<plexus::bounded<policy, 8, 16>, transport>;
using engine       = bounded_node::engine_type;

static_assert(std::is_same_v<engine::peer_storage_type, plexus::io::fixed_peer_storage<8>>);
static_assert(std::is_same_v<engine::topic_storage_type, plexus::graph::fixed_topic_storage<16>>);

}

int main()
{
    return 0;
}
