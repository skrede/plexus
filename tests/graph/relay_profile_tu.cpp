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

#include <utility>
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

// The forwarding-send splice is no longer a profile trait: the engine selects it off the emitter pivot,
// keyed on the ENGINE policy. A relay node's engine carries the wants_refan()-capable real splice; a
// non-relay node's engine carries the members-less null twin (zero forwarding-send code). For a
// single-transport node the engine policy is the profile policy, so the real splice is forward_splice<policy>.
static_assert(std::is_same_v<relay_node::engine_type::forward_splice_type, plexus::io::forward_splice<policy>>);
static_assert(std::is_same_v<plain_node::engine_type::forward_splice_type, plexus::io::null_forward_splice>);

// The offering-side per-origin bookkeeping (the report set, the seq map, and the cooperative-decline
// set) is STATE the real emitter carries; the null twin is a members-less struct, so a non-relay node
// pays zero for it. is_empty pins that structural present/absent — the on-target 0-symbol gate a later
// phase measures rests on the null twin holding no offering state at all.
static_assert(!std::is_empty_v<plexus::io::peer_report_emitter>);
static_assert(std::is_empty_v<plexus::io::null_peer_report_emitter>);

// Both twins mirror the cooperative-decline honor surface so one template parameter threads either
// without a platform branch: the query is const->bool, the mark is (id, bool)->bool. On the null twin
// both are inert no-ops (no decline set to record into); on the real twin they gate the report set.
static_assert(std::is_same_v<decltype(std::declval<const plexus::io::peer_report_emitter &>().declines(std::declval<const plexus::node_id &>())), bool>);
static_assert(std::is_same_v<decltype(std::declval<const plexus::io::null_peer_report_emitter &>().declines(std::declval<const plexus::node_id &>())), bool>);
static_assert(std::is_same_v<decltype(std::declval<plexus::io::peer_report_emitter &>().mark_decline(std::declval<const plexus::node_id &>(), true)), bool>);
static_assert(std::is_same_v<decltype(std::declval<plexus::io::null_peer_report_emitter &>().mark_decline(std::declval<const plexus::node_id &>(), true)), bool>);

// The delivery-path suppression set (the dual-homed consumer's relayed-delivery retirement) lives on
// the message_forwarder, which is keyed on the ENGINE policy — unchanged by relay<>. So the two
// profiles carry the byte-identical forwarder type: the suppression machinery is no relay-only
// structural member, it is inert-by-emptiness on a leaf that never reports an origin.
static_assert(std::is_same_v<relay_node::engine_policy, plain_node::engine_policy>);
static_assert(std::is_same_v<plexus::io::message_forwarder<relay_node::engine_policy>, plexus::io::message_forwarder<plain_node::engine_policy>>);

}

int main()
{
    return 0;
}
