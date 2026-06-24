// The fixed-slot loan pool: borrow/publish lifecycle, alloc-free slot reuse, graceful
// exhaustion, and the publish hand-off that transfers the release obligation to the
// in-flight carrier. A counting type dtor witnesses each release; a borrow on an exhausted
// pool returns an empty loan (never blocks, never grows).
#pragma once

#include "plexus/detail/loan_pool.h"
#include "plexus/io/object_carrier.h"

#include <catch2/catch_test_macros.hpp>

#include <utility>
#include <cstdint>

namespace loan_pool_fixture {

using plexus::detail::loan_pool;

// A T whose construction/destruction is observable through static counters, so a slot's
// release (the in-place dtor) is witnessed without reaching into the pool internals.
struct counted
{
    static inline int live        = 0;
    static inline int constructed = 0;
    std::uint32_t     value;

    explicit counted(std::uint32_t v = 0)
            : value(v)
    {
        ++live;
        ++constructed;
    }
    counted(const counted &o)
            : value(o.value)
    {
        ++live;
        ++constructed;
    }
    ~counted()
    {
        --live;
    }
};

inline void reset_counts()
{
    counted::live        = 0;
    counted::constructed = 0;
}

} // namespace loan_pool_fixture
