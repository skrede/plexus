// The pure request-vs-offered relation oracle: every comparable field's compatibility
// matrix (compatible AND incompatible rows per the offered>=requested direction), the
// always-hard source-identity field refusing regardless of the subscriber's chosen
// mode, strict mode yielding incompatible_qos with the failing-field reason bitmask,
// and permissive mode yielding a degraded verdict with a NON-EMPTY bitmask naming the
// right soft field (the non-silent guarantee at the pure level). Driven directly
// against the relations — no I/O, no registry.
#pragma once

#include "plexus/io/qos_rxo.h"
#include "plexus/io/subscriber_qos.h"
#include "plexus/io/reliability.h"
#include "plexus/topic_qos.h"

#include <catch2/catch_test_macros.hpp>

#include <cstddef>
#include <cstdint>

namespace qos_rxo_fixture {

using plexus::topic_qos;
using plexus::io::durability;
using plexus::io::reliability;
using plexus::io::rxo_mode;
using plexus::io::rxo_verdict;
using plexus::io::subscriber_qos;
using plexus::io::rxo_check;
using plexus::io::reliability_compatible;
using plexus::io::durability_compatible;
using plexus::io::deadline_compatible;
using plexus::io::lease_compatible;
using plexus::io::source_identity_compatible;
using plexus::io::k_rxo_field_reliability;
using plexus::io::k_rxo_field_durability;
using plexus::io::k_rxo_field_deadline;
using plexus::io::k_rxo_field_lease;
using plexus::io::k_rxo_field_max_message_bytes;
using plexus::io::max_message_bytes_compatible;

// The node-level per-message default the size relation resolves an offered topic's
// 0=unset max against. Sized away from the round numbers the cases use so the
// effective-max resolution is observable.
constexpr std::size_t k_test_global_default = 8u * 1024u * 1024u;

} // namespace qos_rxo_fixture
