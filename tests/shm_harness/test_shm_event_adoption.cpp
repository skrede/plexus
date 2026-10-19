#include "support/region_names.h"

#include "plexus/asio/shm/windows/event_doorbell.h"

#include "plexus/shm/region_naming.h"

#include "plexus/testing/platform.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <cstdint>
#include <optional>

#include <windows.h>

namespace {

constexpr std::uint32_t k_slot      = 3;
constexpr std::uint32_t k_first_tag = 41;

bool signaled(HANDLE event)
{
    return ::WaitForSingleObject(event, 0) == WAIT_OBJECT_0;
}

}

// The taken event is manual-reset and signaled, so a zero-timeout wait reads its state without changing it.
TEST_CASE("event_adoption an event already holding the waiter's name is never adopted", "[shm][event_adoption]")
{
    const std::string region = plexus::testing::region_name("shm_harness.event_adoption." + std::to_string(plexus::testing::process_id()), plexus::testing::test_publisher());
    HANDLE taken             = ::CreateEventW(nullptr, TRUE, TRUE, plexus::asio::shm::detail::event_doorbell_name_for(region, k_slot, k_first_tag).data());
    REQUIRE(taken != nullptr);

    std::uint32_t next_tag                                        = k_first_tag;
    const std::optional<plexus::asio::shm::detail::own_event> own = plexus::asio::shm::detail::create_own_event(region, k_slot, [&next_tag] { return next_tag++; });
    const bool taken_signaled                                     = signaled(taken);
    const bool own_signaled                                       = own && signaled(own->handle);
    if(own)
        ::CloseHandle(own->handle);
    ::CloseHandle(taken);

    REQUIRE(own);
    REQUIRE(own->tag == k_first_tag + 1);
    REQUIRE(taken_signaled);
    REQUIRE_FALSE(own_signaled);
}
