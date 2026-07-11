// The MCU-floor -fno-exceptions audit translation unit (FNX-01 / FNX-03).
//
// This TU pulls the core + wire surface an MCU build needs — the routing engine, the
// peer table, the security fail-closed ctors, the wire framing/codec — and compiles it
// under -fno-exceptions -fno-rtti in the build-fnx tree. A real compile is the only
// reliable enumerator of implicit throws (bad_alloc / bad_variant_access /
// bad_optional_access); grep cannot find them. The TU links plexus::core + plexus::inproc
// only — never a facade or a backend — so the include path is exactly the MCU floor.
//
// FNX-03 is STRUCTURAL here: this TU deliberately does NOT include the api facade or the
// host-side observability headers (the recording-handle header and the value-projection
// sink header). Those stay out of the MCU include graph by not being pulled, proven by the
// absence of those includes (a build that needed them would fail header-not-found). Do not
// add them.

#include "plexus/io/routing_engine.h"
#include "plexus/io/known_peers.h"
#include "plexus/io/fixed_peer_storage.h"
#include "plexus/io/reconnect_config.h"
#include "plexus/io/handshake_fsm.h"
#include "plexus/io/liveliness_monitor.h"
#include "plexus/io/peer_liveliness.h"

#include "plexus/io/security/cookie_secret.h"
#include "plexus/io/security/attach_policy.h"

#include "plexus/stream/crc_serial.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/frame_reassembler.h"
#include "plexus/wire/topic_hash.h"

#include "plexus/detail/compat.h"
#include "plexus/detail/fail_closed.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/node_id.h"

#include <span>
#include <chrono>
#include <cstddef>
#include <optional>

// Stand up the routing engine over the inproc policy so the core template machinery —
// including the known_peers table and the wire codec it drives — is instantiated and
// compiled under -fno-exceptions. The proof is structural (compile + link), so the engine
// is merely built and torn down; no flow is driven.
int main()
{
    using policy    = plexus::inproc::inproc_policy;
    using transport = plexus::inproc::inproc_transport<>;

    plexus::inproc::inproc_bus<> bus;
    plexus::inproc::inproc_executor<> executor{bus};
    transport tr{executor, bus};

    plexus::node_id self{};
    self[0] = std::byte{0x01};
    const plexus::io::handshake_fsm_config fsm_cfg{.self_id = self, .version_major = 1, .version_minor = 0, .compatible_version_major = 1, .compatible_version_minor = 0};
    const plexus::io::reconnect_config redial{
            .min_delay = std::chrono::milliseconds{10}, .max_delay = std::chrono::milliseconds{100}, .max_attempts = std::nullopt, .max_elapsed = std::nullopt};

    plexus::log::null_logger sink;
    plexus::io::routing_engine<policy, transport> engine{tr, executor, fsm_cfg, std::chrono::seconds{1}, redial, 0x1u, sink};

    (void)engine.messages();
    (void)engine.known();

    // The constrained-target peer table: instantiate basic_known_peers over the dep-free
    // fixed-capacity storage and drive its four verbs so the over-capacity fail-closed path
    // (plexus::detail::fail_closed) is compiled under -fno-exceptions. The routing_engine
    // threads PeerStorage as a defaulted template param, so a fixed-storage engine compiles
    // on the same MCU floor.
    plexus::io::basic_known_peers<plexus::io::fixed_peer_storage<8>> bounded;
    bounded.note_peer(self, plexus::io::endpoint{.scheme = "inproc", .address = "x"});
    (void)bounded.lookup(self);
    (void)bounded.contains(self);
    bounded.forget(self);

    // The constrained-target fused-liveliness path: instantiate the monitor over the fixed-capacity
    // liveness tables (leases x deadlines) and the arbiter over the fixed-capacity peer table, then
    // drive the ingest verbs WITHIN capacity so their member bodies compile on the -fno-exceptions
    // floor. The over-capacity fail-closed refusal is NOT exercised here — fail_closed aborts under
    // -fno-exceptions, so a directed over-capacity call would abort; that refusal is proved by the
    // exceptions-enabled unit tests instead.
    plexus::io::liveliness_monitor<policy, std::chrono::steady_clock, plexus::io::fixed_liveness_storage<8, 32>> monitor{executor};
    monitor.register_endpoint(self, 0xC0DEu, 1'000'000, 2'000'000);
    monitor.stamp_data(self, 0xC0DEu);
    monitor.stamp_seen(self);
    monitor.deregister_endpoint(self);

    plexus::io::peer_liveliness<plexus::io::fixed_liveliness_peer_storage<8>> arbiter{plexus::io::liveliness_options{}};
    arbiter.note_session_up(self);
    arbiter.note_awareness(self, 0);
    arbiter.note_heartbeat(self, 1'000);
    arbiter.evaluate(2'000);
    arbiter.note_session_down(self, 3'000);

    // Drive the serial CRC decorator's inbound scan/verify path so its member bodies are
    // instantiated and compiled on the -fno-exceptions floor (the MCU build reuses it).
    plexus::stream::crc_serial_inbound dec;
    dec.on_match([](std::span<const std::byte>) {});
    dec.on_drop([](plexus::wire::close_cause) {});
    const std::byte probe[]{std::byte{0x56}, std::byte{0x50}};
    dec.feed(probe);
    return 0;
}
