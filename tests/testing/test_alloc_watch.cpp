#include "support/alloc_watch.h"

#include <catch2/catch_test_macros.hpp>

#include <new>
#include <cstddef>

namespace {

constexpr std::size_t k_watched_bytes = 12289;

// MSVC STL's std::allocator asks operator new for a 4096+ byte request plus 2 * sizeof(void *) + 31 bytes, then aligns
// the block by hand (_Allocate_manually_vector_aligned in <xmemory>).
constexpr std::size_t k_msvc_big_allocation_header = 2 * sizeof(void *) + 31;

}

TEST_CASE("alloc_watch catches a watched block the allocator padded with its alignment header", "[testing]")
{
    plexus::testing::watch_next_alloc(k_watched_bytes);
    void *block = ::operator new(k_watched_bytes + k_msvc_big_allocation_header);
    REQUIRE(plexus::testing::watched_allocated());
    ::operator delete(block);
    REQUIRE(plexus::testing::watched_freed());
}

TEST_CASE("alloc_watch ignores a block smaller than the watched size", "[testing]")
{
    plexus::testing::watch_next_alloc(k_watched_bytes);
    void *block = ::operator new(k_watched_bytes - 1);
    CHECK_FALSE(plexus::testing::watched_allocated());
    ::operator delete(block);
}

TEST_CASE("alloc_watch ignores a block far larger than the watched size", "[testing]")
{
    plexus::testing::watch_next_alloc(k_watched_bytes);
    void *block = ::operator new(2 * k_watched_bytes);
    CHECK_FALSE(plexus::testing::watched_allocated());
    ::operator delete(block);
}
