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
#include "plexus/io/reconnect_config.h"
#include "plexus/io/handshake_fsm.h"

#include "plexus/io/security/cookie_secret.h"
#include "plexus/io/security/attach_policy.h"

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

    plexus::inproc::inproc_bus<>      bus;
    plexus::inproc::inproc_executor<> executor{bus};
    transport                         tr{executor, bus};

    plexus::node_id self{};
    self[0] = std::byte{0x01};
    const plexus::io::handshake_fsm_config fsm_cfg{.self_id                  = self,
                                                   .version_major            = 1,
                                                   .version_minor            = 0,
                                                   .compatible_version_major = 1,
                                                   .compatible_version_minor = 0};
    const plexus::io::reconnect_config     redial{.min_delay    = std::chrono::milliseconds{10},
                                                  .max_delay    = std::chrono::milliseconds{100},
                                                  .max_attempts = std::nullopt,
                                                  .max_elapsed  = std::nullopt};

    plexus::io::routing_engine<policy, transport> engine{
            tr, executor, fsm_cfg, std::chrono::seconds{1}, redial, 0x1u};

    (void)engine.messages();
    (void)engine.known();
    return 0;
}
