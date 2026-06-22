#include "plexus/inproc/inproc_transport.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_bus.h"

#include "plexus/io/routing_engine.h"
#include "plexus/io/reconnect_config.h"
#include "plexus/io/handshake_fsm.h"

#include "plexus/node_id.h"

#include <chrono>
#include <cstddef>
#include <optional>

// The MCU-minimal link surface: this translation unit links ONLY plexus::core and
// plexus::inproc — never a facade or a compiled backend — and the include path is
// restricted to their two include trees. Instantiating the routing_engine over the
// inproc policy forces the core's template machinery to compile; if any core header
// ever reached for a facade or a backend header, the build would fail header-not-found.
// The proof is structural, not behavioral, so the engine is merely stood up and torn
// down — no flow is driven.
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

    plexus::log::null_logger                      sink;
    plexus::io::routing_engine<policy, transport> engine{
            tr, executor, fsm_cfg, std::chrono::seconds{1}, redial, 0x1u, sink};

    (void)engine.messages();
    return 0;
}
