// The relay-decorator vocabulary gate: a profile never spelled relay<> resolves the peer_report
// emitter to the structurally-absent null twin, composing relay<> onto it (and only that) selects the
// real emitter while preserving every storage/log alias, and both spellings still satisfy
// target_profile. All proofs are compile-time; main() only signals. The on-target 0-symbol gate the
// null twin backs is a later phase's measurement — this is the host-side presence/absence proof.

#include "plexus/target_profile.h"

#include "plexus/node.h"

#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/io/fixed_peer_storage.h"

#include <type_traits>

namespace {

using policy    = plexus::inproc::inproc_policy;
using transport = plexus::inproc::inproc_transport<>;

using plain  = plexus::detail::profile_traits<policy>;
using capped = plexus::detail::profile_traits<plexus::bounded<policy, 8, 16>>;

// A profile never spelled relay<> carries the null emitter: no peer_report is minted or sent.
static_assert(std::is_same_v<plain::peer_report_emitter, plexus::io::null_peer_report_emitter>);
static_assert(std::is_same_v<capped::peer_report_emitter, plexus::io::null_peer_report_emitter>);

using relayed        = plexus::detail::profile_traits<plexus::relay<policy>>;
using relayed_capped = plexus::detail::profile_traits<plexus::relay<plexus::bounded<policy, 8, 16>>>;

// Composing relay<> is the single spelling that turns the real emitter on.
static_assert(std::is_same_v<relayed::peer_report_emitter, plexus::io::peer_report_emitter>);
static_assert(std::is_same_v<relayed_capped::peer_report_emitter, plexus::io::peer_report_emitter>);

// relay<> overrides ONLY the emitter: every other alias is the wrapped profile's, so relay<bounded<>>
// keeps the fixed twins and relay<policy> keeps the heap twins.
static_assert(std::is_same_v<relayed::policy, plain::policy>);
static_assert(std::is_same_v<relayed::peer_storage, plain::peer_storage>);
static_assert(std::is_same_v<relayed::topic_storage, plain::topic_storage>);
static_assert(std::is_same_v<relayed::graph_change_log, plain::graph_change_log>);
static_assert(std::is_same_v<relayed_capped::peer_storage, plexus::io::fixed_peer_storage<8>>);
static_assert(std::is_same_v<relayed_capped::topic_storage, plexus::graph::fixed_topic_storage<16>>);

// Both spellings resolve a valid policy, so a relayed profile is a target_profile the node accepts.
static_assert(plexus::detail::target_profile<plexus::relay<policy>>);
static_assert(plexus::detail::target_profile<plexus::relay<plexus::bounded<policy, 8, 16>>>);

// Through the facade the relayed node's engine binds the real emitter, a non-relay node the null one.
using relay_node = plexus::node<plexus::relay<policy>, transport>;
using plain_node = plexus::node<policy, transport>;
static_assert(std::is_same_v<relay_node::engine_type::peer_report_emitter_type, plexus::io::peer_report_emitter>);
static_assert(std::is_same_v<plain_node::engine_type::peer_report_emitter_type, plexus::io::null_peer_report_emitter>);

}

int main()
{
    return 0;
}
