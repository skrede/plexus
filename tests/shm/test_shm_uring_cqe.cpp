#include "support/heap_ring.h"

#include "plexus/asio/shm/linux/uring_cqe.h"
#include "plexus/asio/shm/linux/ring_notifier.h"

#include "plexus/asio/asio_policy.h"

#include "plexus/shm/arm_status.h"
#include "plexus/shm/slot_subscriber.h"
#include "plexus/shm/fallback_notifier.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <cerrno>
#include <cstdint>
#include <type_traits>

namespace {

using plexus::asio::asio_policy;
using plexus::asio::shm::cqe_verdict;
using plexus::asio::shm::classify_futex_cqe;

cqe_verdict expected_verdict(std::int32_t res)
{
    if(res >= 0 || res == -EAGAIN || res == -EINTR)
        return cqe_verdict::rearm;
    return res == -ECANCELED ? cqe_verdict::stop : cqe_verdict::demote;
}

}

TEST_CASE("uring_cqe a futex wait completion classifies over every errno and success", "[shm][uring_cqe]")
{
    for(std::int32_t res = -4095; res <= 1; ++res)
        REQUIRE(classify_futex_cqe(res) == expected_verdict(res));
}

TEST_CASE("uring_cqe an unsupported futex wait demotes instead of re-arming", "[shm][uring_cqe]")
{
    CHECK(classify_futex_cqe(-EINVAL) == cqe_verdict::demote);
    CHECK(classify_futex_cqe(-ENOSYS) == cqe_verdict::demote);
    CHECK(classify_futex_cqe(-EOPNOTSUPP) == cqe_verdict::demote);
    CHECK(classify_futex_cqe(-EAGAIN) == cqe_verdict::rearm);
    CHECK(classify_futex_cqe(-EINTR) == cqe_verdict::rearm);
    CHECK(classify_futex_cqe(-ECANCELED) == cqe_verdict::stop);
    CHECK(classify_futex_cqe(0) == cqe_verdict::rearm);
}

TEST_CASE("uring_cqe the production notifier engages io_uring on a kernel with futex wait", "[shm][uring_cqe]")
{
    using production = plexus::asio::shm::ring_notifier<asio_policy>;
    using composite  = plexus::shm::fallback_notifier<plexus::asio::shm::ring_notifier_uring<asio_policy>, plexus::asio::shm::ring_notifier_threaded<asio_policy>>;
    STATIC_REQUIRE(std::is_same_v<production, composite>);

    ::asio::io_context io;
    plexus::testing::heap_ring heap{4, 64};
    REQUIRE(heap.created());
    plexus::shm::slot_subscriber sub(heap.ring());
    production notifier(io, heap.ring().header(), "");

    REQUIRE(notifier.arm(*sub.cursor_index(), [] {}) == plexus::shm::arm_status::armed);
    CHECK(notifier.engaged() == plexus::shm::fallback_tier::primary);
    CHECK(notifier.primary_status() == plexus::shm::arm_status::armed);
    notifier.disarm();
}
