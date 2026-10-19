#include "child_args.h"

#include "support/xproc_harness.h"

#include "plexus/native/shm_region_broker.h"

#include "plexus/shm/slot_liveness.h"
#include "plexus/shm/region_broker_concept.h"

#include <chrono>
#include <string>
#include <thread>
#include <vector>
#include <cstdint>
#include <optional>

#if defined(__linux__)
    #include <csignal>

    #include <sys/prctl.h>
#endif

namespace {

// Attaches the region the parent created and holds the slot it names until the parent kills it; the sleep is bounded
// so a child whose parent died ends on its own where no parent-death signal exists.
bool slot_holder_child(const std::vector<std::string> &argv)
{
#if defined(__linux__)
    ::prctl(PR_SET_PDEATHSIG, SIGKILL);
#endif
    const std::optional<std::uint32_t> slot = argv.size() == 2 ? shm_harness::to_u32(argv[1]) : std::nullopt;
    if(!slot)
        return false;
    plexus::native::shm_region_broker broker;
    plexus::native::shm_region_handle region;
    if(broker.attach(argv[0], region) != plexus::shm::region_status::ok || region.hold_slot(*slot) != plexus::shm::slot_hold::held)
        return false;
    std::this_thread::sleep_for(std::chrono::seconds(60));
    return true;
}

[[maybe_unused]] const bool s_slot_holder_registered = plexus::testing::register_xproc_child("shm_harness.slot_holder", &slot_holder_child);

}
