#ifndef HPP_GUARD_PLEXUS_SHM_RING_HEADER_H
#define HPP_GUARD_PLEXUS_SHM_RING_HEADER_H

#include "plexus/node_id.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace plexus::shm {

// Cache-line size used to pad the hot atomics and the descriptor cells onto
// their own lines, suppressing false sharing between producers and consumers.
//
// This is a CROSS-PROCESS LAYOUT field: every process (and every build) mapping
// the region must agree on it byte-for-byte, so it MUST be a stable literal
// constant -- it must NOT be the std destructive-interference hint, whose value
// can vary between compiler versions and -mtune/-mcpu flags (the compiler
// rejects naming that hint in an ABI position for exactly this reason). It is
// fixed at the host-verified destructive-interference size; a consuming test TU
// may assert this value still covers the target's std hint, where that reference
// is ABI-safe.
inline constexpr std::size_t k_cache_line = 64;

// Upper bound on registered broadcast consumers. The cursor array is a
// fixed-bound, in-region slot so the layout stays stable across processes; its
// per-cursor semantics (the min-cursor reclamation gate) drive the cursor-gated
// reclamation in broadcast_ring. A fixed bound keeps every in-region struct
// pointer-free and standard-layout.
inline constexpr std::size_t k_max_consumers = 16;

// Magic + version word stamped by the creator into the control header and
// re-validated by an attacher before it trusts any other header field. A
// mismatch means the region is foreign or a layout-incompatible build.
inline constexpr std::uint64_t k_ring_magic = 0x50'4c'58'52'4e'47'35'00ull; // "PLXRNG5\0"

// The cursor position of a slot registered but not yet gating: a fresh cursor does not hold back
// reclamation until it reads its first slot.
inline constexpr std::uint64_t k_cursor_idle = UINT64_MAX;

// The 3-state encoding of each cursor slot's park word (a SEPARATE atom from
// notify_generation: the generation says WHAT to drain, the park word says WHETHER
// that cursor's waiter is asleep). The waiter blocks on its own word while it reads
// PARKED, and a producer wakes it only on a PARKED -> NOTIFIED exchange, so a
// spinning (EMPTY) waiter costs the producer no wake syscall. EMPTY is the
// zero-init default so a freshly created header parks nobody without a store.
// (Reference: the std/parking_lot 3-state thread-parking word.)
inline constexpr std::uint32_t k_park_empty    = 0;
inline constexpr std::uint32_t k_park_parked   = 1;
inline constexpr std::uint32_t k_park_notified = 2;

#if defined(_MSC_VER)
    #pragma warning(push)
    // The alignas on these cross-process control structs is deliberate cache-line isolation;
    // the padding MSVC warns about under /W4 is the intended layout, not an accident.
    #pragma warning(disable : 4324)
#endif

// One broadcast consumer's read cursor, on its own cache line so a consumer
// advancing its cursor does not thrash the line of an adjacent consumer or the
// producer's enqueue counter. `position` is a 64-bit monotonically-advancing
// index, mirroring the sequence width. `active` marks a registered slot.
// `waiter_tag` names the waiter's own doorbell where the platform wakes through
// a named kernel object instead of a futex.
struct alignas(k_cache_line) cursor_t
{
    std::atomic<std::uint64_t> position;
    std::atomic<std::uint32_t> active;
    std::atomic<std::uint32_t> waiter_tag;
};

// Control header placement-new'd at the front of the control+cells region. The
// single hot producer counter (enqueue_pos) sits on its own cache line; the
// immutable config block (two lines) and the consumer-cursor array each start on
// their own line so none of them shares with the producer counter. The cells follow the
// header in the same region (see control_region_bytes); the payload slab is a
// SEPARATE region so its base is independently page-aligned and slot i is at a
// pure i*slot_capacity offset (every slot base stays 8-aligned for the codec
// zero-copy alias precondition).
struct control_header_t
{
    alignas(k_cache_line) std::atomic<std::uint64_t> enqueue_pos;

    alignas(k_cache_line) std::atomic<std::uint64_t> magic;
    std::uint64_t cell_count;    // power of two
    std::uint64_t slot_capacity; // multiple of 8
    std::uint64_t mask;          // cell_count - 1

    // The per-ring LOGICAL consumer bound. depth was derived to strictly exceed it
    // (the reclaim-headroom invariant), and the cursor verbs scan only this many of
    // the fixed cursor slots, so a ring declared for few consumers neither over-sizes
    // its depth nor touches cursor lines it does not use. The physical cursors array
    // below stays the fixed cap, so this is a bound over a fixed region, never a
    // variable-length tail: any value in [1, k_max_consumers] is representable.
    std::uint64_t consumer_capacity;

    // The region sizes exactly as the creator's broker mapped them and the publishing node's id, so an attacher
    // checks the regions it mapped and the ring's owner before trusting any size it computes from the header.
    std::uint64_t control_bytes;
    std::uint64_t slab_bytes;
    node_id publisher;

    alignas(k_cache_line) cursor_t cursors[k_max_consumers];

    // Monotonic wakeup-generation counter for the cross-process consumer
    // notifier. ANY producer process bumps it after publishing; a waiter
    // snapshots it before it drains and re-checks it after storing PARKED, so a
    // bump that lands between the drain and the block keeps the waiter awake. It
    // lives on its OWN cache line, mirroring enqueue_pos, so producer wakeups do
    // not thrash the consumer-cursor lines.
    alignas(k_cache_line) std::atomic<std::uint32_t> notify_generation;

    // One park word per cursor slot, packed on ONE line apart from the cursors: a
    // waiter writes its word only when it parks or unparks, so the producer's wake
    // gate reads a single clean line however busy the consumers' cursor lines are.
    alignas(k_cache_line) std::atomic<std::uint32_t> park[k_max_consumers];

    // Monotonic high-water of registered broadcast cursors: the largest (index + 1)
    // any register_cursor has ever claimed. The reliable-reclaim scan bounds its
    // loop by this instead of always k_max_consumers, so a 1-2 consumer ring touches
    // 1-2 cursor lines per claim, not 16. It only ever grows (an unregister leaves
    // it), so a stale registered cursor is always below the high-water it set and
    // the scan can never miss it; the dense ascending allocation keeps it near-tight.
    // On its OWN cache line so a register-time bump never thrashes the producer's
    // generation line.
    alignas(k_cache_line) std::atomic<std::uint32_t> high_water;

    constexpr std::size_t cursor_region_capacity() const noexcept
    {
        return static_cast<std::size_t>(consumer_capacity);
    }
};

#if defined(_MSC_VER)
    #pragma warning(pop)
#endif

// Cross-process validity gates. A non-lock-free atomic on the target would
// silently route through a process-local lock table -- two processes mapping the
// same region would then NOT synchronize -- so a non-lock-free target must fail
// to compile here. The in-region structs must be standard-layout (no virtuals,
// no owning members, no pointers) for placement-new-once at a fixed offset to be
// well-defined across address spaces.
static_assert(std::atomic<std::uint64_t>::is_always_lock_free, "ring sequence/position atomics must be always-lock-free across processes");
static_assert(std::atomic<std::uint32_t>::is_always_lock_free, "ring refcount/flag atomics must be always-lock-free across processes");
static_assert(std::is_standard_layout_v<cursor_t>, "cursor_t must be standard-layout for SHM placement");
static_assert(std::is_standard_layout_v<control_header_t>, "control_header_t must be standard-layout for SHM placement");

// Pin the cross-process layout: enqueue_pos line + two config lines + cursor array +
// notify_generation line + park line + high_water line, each cache-line-aligned. An
// unintended field add/reorder (which would shift every in-region offset and
// silently mismap a peer) fails the build here. A deliberate layout change updates
// this size AND bumps k_ring_magic so an old region fails-closed on attach.
static_assert(sizeof(control_header_t) == k_cache_line * 3 + k_max_consumers * sizeof(cursor_t) + k_cache_line * 3,
              "control_header_t layout drift -- update the size guard and bump k_ring_magic");
static_assert(sizeof(cursor_t) == k_cache_line, "cursor_t must stay one cache line");
static_assert(sizeof(control_header_t::park) == k_cache_line && alignof(control_header_t) >= k_cache_line, "the park words must fill exactly one cache line");
static_assert(offsetof(control_header_t, park) == k_cache_line * 4 + k_max_consumers * sizeof(cursor_t), "the park line must follow the notify_generation line");
static_assert(offsetof(control_header_t, publisher) + sizeof(node_id) <= offsetof(control_header_t, cursors), "the config block must end before the cursor array");
static_assert(k_max_consumers <= 32, "a 32-bit parked-slot mask must cover every cursor");
static_assert(k_cursor_idle == UINT64_MAX, "an idle (registered-but-not-gating) cursor must read as the max sentinel");

}

#endif
