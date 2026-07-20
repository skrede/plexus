// The profile-vocabulary gate: a bare mechanism policy resolves to the heap storage twins, a
// bounded<> decorator resolves the graph tables to the fixed twins with the policy preserved, and a
// capacity integer is separable from the policy. All proofs are compile-time; main() only signals.

#include "plexus/target_profile.h"

#include "plexus/inproc/inproc_policy.h"

#include <type_traits>

namespace
{

using policy = plexus::inproc::inproc_policy;

using heap = plexus::detail::profile_traits<policy>;
static_assert(std::is_same_v<heap::policy, policy>);
static_assert(std::is_same_v<heap::peer_storage, plexus::io::std_map_peer_storage>);
static_assert(std::is_same_v<heap::topic_storage, plexus::graph::std_map_topic_storage>);
static_assert(std::is_same_v<heap::liveliness_storage, plexus::io::default_liveliness_storage>);
static_assert(plexus::detail::target_profile<policy>);

using small = plexus::detail::profile_traits<plexus::bounded<policy, 8, 16>>;
static_assert(std::is_same_v<small::policy, policy>);
static_assert(std::is_same_v<small::peer_storage, plexus::io::fixed_peer_storage<8>>);
static_assert(std::is_same_v<small::topic_storage, plexus::graph::fixed_topic_storage<16>>);
static_assert(std::is_same_v<small::liveliness_storage, plexus::io::default_liveliness_storage>);
static_assert(plexus::detail::target_profile<plexus::bounded<policy, 8, 16>>);

using tiny = plexus::detail::profile_traits<plexus::bounded<policy, 4, 4>>;
static_assert(std::is_same_v<tiny::policy, small::policy>);
static_assert(!std::is_same_v<tiny::peer_storage, small::peer_storage>);
static_assert(!std::is_same_v<tiny::topic_storage, small::topic_storage>);

using bundle = plexus::fixed_liveliness<8, 32, 8>;
static_assert(std::is_same_v<bundle::monitor, plexus::io::fixed_liveness_storage<8, 32>>);
static_assert(std::is_same_v<bundle::arbiter, plexus::io::fixed_liveliness_peer_storage<8>>);

using bounded_live = plexus::detail::profile_traits<plexus::bounded<policy, 8, 16, bundle>>;
static_assert(std::is_same_v<bounded_live::liveliness_storage, bundle>);
static_assert(std::is_same_v<bounded_live::peer_storage, plexus::io::fixed_peer_storage<8>>);

}

int main()
{
    return 0;
}
