// The liveliness-monitor storage-backend oracle: proves the two monitor tables (the
// per-(node,topic) deadline table and the per-node lease table) keep one verb surface
// across the std::map default (the PC type) and a dep-free fixed-capacity twin, and that
// the fixed twin fails closed on the (N+1)-th distinct key of EITHER table. A shared
// insert/stamp/erase script drives both backends to identical find/for_each results, and
// a steady-state re-stamp is proven alloc-free on both. No socket, no backend — the
// header-only core linked against plexus::core + Catch2's main only. The alloc gate
// overrides global operator new (support/alloc_counter.h), so this TU stands alone.

#include "plexus/io/liveness_storage.h"

#include "plexus/io/detail/endpoint_liveness.h"

#include "plexus/node_id.h"

#include "support/alloc_counter.h"

#include <catch2/catch_test_macros.hpp>

#include <vector>
#include <cstddef>
#include <cstdint>
#include <algorithm>

using plexus::node_id;
using plexus::io::fixed_liveness_storage;
using plexus::io::std_map_liveness_storage;
using plexus::io::detail::deadline_key;
using plexus::io::detail::endpoint_liveness;

namespace {

node_id id_of(std::uint8_t b)
{
    node_id id{};
    id[0] = std::byte{b};
    return id;
}

deadline_key key_of(std::uint8_t b, std::uint64_t topic)
{
    return deadline_key{id_of(b), topic};
}

// The last_seen_ns of every lease slot, sorted — an order-independent fingerprint of
// the lease table that the std::map and flat-array backends must agree on.
template<typename Storage>
std::vector<std::uint64_t> lease_stamps(Storage &s)
{
    std::vector<std::uint64_t> out;
    s.for_each_lease([&](const node_id &, endpoint_liveness &e) { out.push_back(e.last_seen_ns); });
    std::sort(out.begin(), out.end());
    return out;
}

template<typename Storage>
void run_script(Storage &s)
{
    s.upsert_deadline(key_of(0x01, 0xAA), 300, 10);
    s.upsert_lease(id_of(0x01), 500, 10);
    s.upsert_deadline(key_of(0x01, 0xBB), 300, 10);
    s.upsert_lease(id_of(0x02), 500, 20);
    s.upsert_lease(id_of(0x02), 500, 40); // re-stamp: same key, no new slot
    s.erase_endpoint(id_of(0x01));         // drops both 0x01 deadlines + the 0x01 lease
}

} // namespace

TEST_CASE("liveness_storage fixed twin fills to capacity and reuses a freed slot", "[io][liveness_storage]")
{
    fixed_liveness_storage<2, 2> s;

    s.upsert_lease(id_of(0x01), 500, 10);
    s.upsert_lease(id_of(0x02), 500, 10);
    REQUIRE(s.find_lease(id_of(0x01)) != nullptr);
    REQUIRE(s.find_lease(id_of(0x02)) != nullptr);

    s.upsert_deadline(key_of(0x01, 0xAA), 300, 10);
    s.upsert_deadline(key_of(0x02, 0xBB), 300, 10);
    REQUIRE(s.find_deadline(key_of(0x01, 0xAA)) != nullptr);
    REQUIRE(s.find_deadline(key_of(0x02, 0xBB)) != nullptr);

    // Freeing 0x01 reopens one slot in BOTH tables; a fresh distinct key fits again.
    s.erase_endpoint(id_of(0x01));
    REQUIRE(s.find_lease(id_of(0x01)) == nullptr);
    REQUIRE(s.find_deadline(key_of(0x01, 0xAA)) == nullptr);
    s.upsert_lease(id_of(0x09), 500, 30);
    s.upsert_deadline(key_of(0x09, 0xCC), 300, 30);
    REQUIRE(s.find_lease(id_of(0x09)) != nullptr);
    REQUIRE(s.find_deadline(key_of(0x09, 0xCC)) != nullptr);
}

TEST_CASE("liveness_storage fixed twin fails closed on the (N+1)-th distinct lease key", "[io][liveness_storage]")
{
    fixed_liveness_storage<2, 4> s;
    s.upsert_lease(id_of(0x01), 500, 10);
    s.upsert_lease(id_of(0x02), 500, 10);
    REQUIRE_THROWS_AS(s.upsert_lease(id_of(0x03), 500, 10), std::runtime_error);
}

TEST_CASE("liveness_storage fixed twin fails closed on the (N+1)-th distinct deadline key", "[io][liveness_storage]")
{
    fixed_liveness_storage<4, 2> s;
    s.upsert_deadline(key_of(0x01, 0xAA), 300, 10);
    s.upsert_deadline(key_of(0x01, 0xBB), 300, 10);
    REQUIRE_THROWS_AS(s.upsert_deadline(key_of(0x02, 0xCC), 300, 10), std::runtime_error);
}

TEST_CASE("liveness_storage the two backends agree over an insert/stamp/erase script", "[io][liveness_storage]")
{
    std_map_liveness_storage heap;
    fixed_liveness_storage<4, 4> fixed;
    run_script(heap);
    run_script(fixed);

    REQUIRE(lease_stamps(heap) == lease_stamps(fixed));

    // The surviving 0x02 lease carries its latest re-stamp on both; the erased 0x01 is gone.
    REQUIRE(heap.find_lease(id_of(0x01)) == nullptr);
    REQUIRE(fixed.find_lease(id_of(0x01)) == nullptr);
    REQUIRE(heap.find_lease(id_of(0x02))->last_seen_ns == 40);
    REQUIRE(fixed.find_lease(id_of(0x02))->last_seen_ns == 40);
    REQUIRE(heap.find_deadline(key_of(0x01, 0xAA)) == nullptr);
    REQUIRE(fixed.find_deadline(key_of(0x01, 0xAA)) == nullptr);
}

TEST_CASE("liveness_storage a steady-state re-stamp allocates nothing on either backend", "[io][liveness_storage]")
{
    std_map_liveness_storage heap;
    fixed_liveness_storage<4, 4> fixed;
    heap.upsert_lease(id_of(0x01), 500, 10);
    fixed.upsert_lease(id_of(0x01), 500, 10);

    plexus::testing::reset_alloc_count();
    for(int i = 0; i < 1000; ++i)
    {
        if(endpoint_liveness *l = heap.find_lease(id_of(0x01)))
            l->last_seen_ns = static_cast<std::uint64_t>(i);
        heap.upsert_lease(id_of(0x01), 500, static_cast<std::uint64_t>(i));
        if(endpoint_liveness *l = fixed.find_lease(id_of(0x01)))
            l->last_seen_ns = static_cast<std::uint64_t>(i);
        fixed.upsert_lease(id_of(0x01), 500, static_cast<std::uint64_t>(i));
    }
    REQUIRE(plexus::testing::alloc_count() == 0);
}
