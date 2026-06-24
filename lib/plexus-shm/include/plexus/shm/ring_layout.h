#ifndef HPP_GUARD_PLEXUS_SHM_RING_LAYOUT_H
#define HPP_GUARD_PLEXUS_SHM_RING_LAYOUT_H

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace plexus::shm {

// over-limit: cited cross-process ring layout + named lock-free algorithms (std/parking_lot
// 3-state word, Vyukov, Dekker mutual-announce) + the cross-process layout invariants; the why
// is load-bearing (the keep-list) and trimming the citations would lose the ABI/ordering record.

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
inline constexpr std::uint64_t k_ring_magic = 0x50'4c'58'52'4e'47'33'00ull; // "PLXRNG3\0"

// The 3-state encoding of the parked-waiter word in control_header_t (a SEPARATE
// atom from notify_generation: the generation says WHAT to drain, the park-state
// says WHETHER anyone is asleep). A producer reads it to skip the FUTEX_WAKE
// syscall when no consumer is parked; the canonical EMPTY/PARKED/NOTIFIED futex
// parking encoding rules out the lost wakeup when the consumer stores PARKED
// (release) before committing to the wait and re-checks the generation. EMPTY is
// the zero-init default so a freshly created header parks nobody without a store.
// (Reference: the std/parking_lot 3-state thread-parking word.)
inline constexpr std::uint32_t k_park_empty    = 0;
inline constexpr std::uint32_t k_park_parked   = 1;
inline constexpr std::uint32_t k_park_notified = 2;

// payload_len sentinel for the SKIPPED tombstone state (see cell_t). A best-effort
// producer that must donate its position past a pinned cell advances that cell's
// sequence to (pos + 1) for Vyukov recognition but stamps this length so the
// consumer recognizes a non-deliverable tombstone and steps its cursor forward
// without delivering. A real message length is always <= slot_capacity, far below
// this value, so the two never collide.
inline constexpr std::uint32_t k_skip_len = 0xFFFF'FFFFu;

// One descriptor cell of the Vyukov bounded MPMC ring. It stores a DESCRIPTOR,
// never the payload bytes and never a pointer: the control+cells region and the
// payload slab map at different virtual addresses in each process, so a cell
// carries payload_offset (a byte offset into the slab), not an address.
//
// `sequence` is the Vyukov per-cell sequence AND the generation counter in one
// 64-bit field: it encodes both the index and the lap, so a slow consumer
// detects a recycled cell via an int64 difference rather than aliasing an old
// lap with a new one. It is 64-bit on purpose -- a narrower counter wraps and
// aliases laps (the prior narrow-sequence wrap bug class). The producer writes
// payload_offset/payload_len BEFORE the release store of `sequence`; a consumer
// loads `sequence` with acquire BEFORE reading the descriptor.
//
// SEQUENCE STATES for a cell whose slot serves ring position `pos` (its prior
// occupant was at `pos - cell_count`). With dif = (int64)sequence - (int64)pos:
//   dif == 0                -> FREE: fresh first-lap cell, claimable by a
//                              reliable or best-effort producer.
//   dif == 1 - cell_count   -> FILLED-LAST-LAP: the prior occupant committed and
//                              has been lapped; a best-effort overwrite may
//                              recycle this slot (its bytes are stale).
//   dif == -cell_count      -> IN-FLIGHT-PEER: another producer claimed this slot
//                              for `pos` but has not committed yet; a best-effort
//                              overwrite MUST NOT steal it (the peer is writing).
//   dif == 1                -> FILLED: committed at `pos`, deliverable to a
//                              consumer whose cursor == pos.
//   dif == 1 + cell_count   -> SKIPPED: this slot was donated past a pinned cell
//                              by a best-effort producer. Its sequence was advanced
//                              to (pos + 1) so a future claim recognizes it, but it
//                              carries no deliverable payload -- consume() detects
//                              this tombstone and steps its cursor forward WITHOUT
//                              delivering. It is the smallest Vyukov-compatible
//                              extension that lets an overwrite bypass a pinned cell
//                              while keeping the sequence sequence-recognizable.
// The reclaiming intent marker a best-effort overwrite publishes BEFORE touching
// the payload is `pos` itself (transitioning the cell OUT of FILLED-LAST-LAP back
// to the dif==0 marker for `pos`), so a racing peer or a pinning reader observes
// the slot leave its filled state before any byte is written.
//
// payload_offset/payload_len are atomic and accessed memory_order_relaxed UNDER
// the seq release/acquire pairing: the seq store/load is the real synchronization
// edge that orders the payload-byte writes, so the descriptor fields need no
// independent ordering, but they must be atomic so a best-effort lap-overwrite
// racing a stale-cursor reader is not a data race (a benign-but-real report TSan
// would otherwise flag on the raw scalar).
//
// `take_refcount` pins the slot while a consumer holds an outstanding read. The
// best-effort overwrite and the consumer's take() form a Dekker mutual-announce
// pair over (sequence, take_refcount): the producer announces overwrite-intent on
// sequence then checks take_refcount; the reader announces its pin on take_refcount
// then re-checks sequence. Both announce-stores and both check-loads are seq_cst,
// so store-buffering cannot let both sides proceed past a live pin.
struct alignas(k_cache_line) cell_t
{
    std::atomic<std::uint64_t> sequence;
    std::atomic<std::uint64_t> payload_offset;
    std::atomic<std::uint32_t> payload_len;
    std::atomic<std::uint32_t> take_refcount;
};

// One broadcast consumer's read cursor, on its own cache line so a consumer
// advancing its cursor does not thrash the line of an adjacent consumer or the
// producer's enqueue counter. `position` is a 64-bit monotonically-advancing
// index, mirroring the sequence width. `active` marks a registered slot.
struct alignas(k_cache_line) cursor_t
{
    std::atomic<std::uint64_t> position;
    std::atomic<std::uint32_t> active;
};

// Control header placement-new'd at the front of the control+cells region. The
// single hot producer counter (enqueue_pos) sits on its own cache line; the
// immutable config block and the consumer-cursor array each start on their own
// line so none of them shares with the producer counter. The cells follow the
// header in the same region (see control_region_bytes); the payload slab is a
// SEPARATE region so its base is independently page-aligned and slot i is at a
// pure i*slot_capacity offset (every slot base stays 8-aligned for the codec
// zero-copy alias precondition).
struct control_header_t
{
    alignas(k_cache_line) std::atomic<std::uint64_t> enqueue_pos;

    alignas(k_cache_line) std::uint64_t magic;
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

    alignas(k_cache_line) cursor_t cursors[k_max_consumers];

    // Monotonic wakeup-generation counter for the cross-process consumer
    // notifier. ANY producer process bumps it (release) after publishing and
    // wakes a waiter blocked on it by address; the consumer reads it, drains,
    // then waits on the last-seen value so a bump that lands between drain and
    // wait is never lost. It lives on its OWN cache line, mirroring enqueue_pos,
    // so producer wakeups do not thrash the consumer-cursor line. It is a 4-byte
    // lock-free atomic (== the kernel futex word width) so the by-address wakeup
    // syscall operates on it directly across address spaces.
    alignas(k_cache_line) std::atomic<std::uint32_t> notify_generation;

    // The parked-waiter 3-state (k_park_empty/parked/notified). A consumer's
    // notifier park boundary stores PARKED (release) before committing to the futex
    // wait; the producer's gated wake swaps it to NOTIFIED and issues FUTEX_WAKE
    // ONLY when the prior state was PARKED, so a busy/spinning (EMPTY) consumer costs
    // the producer zero wake syscalls. It lives on its OWN cache line so the
    // producer's wake-gate read and the consumer's park store never thrash the
    // generation line. A distinct word from notify_generation on purpose: folding
    // the bit into the generation would couple the kernel FUTEX_WAIT value-compare
    // to the park state and risk a lost wake.
    alignas(k_cache_line) std::atomic<std::uint32_t> park_state;

    // Monotonic high-water of registered broadcast cursors: the largest (index + 1)
    // any register_cursor has ever claimed. The reliable-reclaim scan bounds its
    // loop by this instead of always k_max_consumers, so a 1-2 consumer ring touches
    // 1-2 cursor lines per claim, not 16. It only ever grows (an unregister leaves
    // it), so a stale registered cursor is always below the high-water it set and
    // the scan can never miss it; the dense ascending allocation keeps it near-tight.
    // On its OWN cache line so a register-time bump never thrashes the producer's
    // park/generation lines.
    alignas(k_cache_line) std::atomic<std::uint32_t> high_water;

    [[nodiscard]] constexpr std::size_t cursor_region_capacity() const noexcept
    {
        return static_cast<std::size_t>(consumer_capacity);
    }
};

// Cross-process validity gates. A non-lock-free atomic on the target would
// silently route through a process-local lock table -- two processes mapping the
// same region would then NOT synchronize -- so a non-lock-free target must fail
// to compile here. The in-region structs must be standard-layout (no virtuals,
// no owning members, no pointers) for placement-new-once at a fixed offset to be
// well-defined across address spaces.
static_assert(std::atomic<std::uint64_t>::is_always_lock_free,
              "ring sequence/position atomics must be always-lock-free across processes");
static_assert(std::atomic<std::uint32_t>::is_always_lock_free,
              "ring refcount/flag atomics must be always-lock-free across processes");
static_assert(std::is_standard_layout_v<cell_t>,
              "cell_t must be standard-layout for SHM placement");
static_assert(std::is_standard_layout_v<cursor_t>,
              "cursor_t must be standard-layout for SHM placement");
static_assert(std::is_standard_layout_v<control_header_t>,
              "control_header_t must be standard-layout for SHM placement");

// Pin the cross-process layout: enqueue_pos line + config line + cursor array +
// notify_generation line + park_state line + high_water line, each
// cache-line-aligned. consumer_capacity rides the existing config line (it fits the
// 64 bytes alongside magic/cell_count/slot_capacity/mask), so adding it does not
// grow the header and this size formula is unchanged. An unintended field add/reorder
// (which would shift every in-region offset and silently mismap a peer) fails the
// build here. A deliberate layout change updates this size AND bumps k_ring_magic so
// an old region fails-closed on attach.
static_assert(sizeof(control_header_t) ==
                      k_cache_line * 2 + k_max_consumers * sizeof(cursor_t) + k_cache_line * 3,
              "control_header_t layout drift -- update the size guard and bump k_ring_magic");

// Round a byte count up to the next multiple of eight so a slot base laid out
// at i*slot_capacity, over a page-aligned slab base, is always 8-aligned.
constexpr std::size_t round_up_8(std::size_t bytes) noexcept
{
    return (bytes + 7u) & ~static_cast<std::size_t>(7u);
}

// Bytes the control+cells region must hold: the header followed by cell_count
// cache-line-aligned cells.
constexpr std::size_t control_region_bytes(std::size_t cell_count) noexcept
{
    return sizeof(control_header_t) + cell_count * sizeof(cell_t);
}

// Bytes the payload slab region must hold: cell_count fixed-stride slots, each
// slot_capacity rounded up to a multiple of eight so slot i at i*stride is
// 8-aligned.
constexpr std::size_t slab_region_bytes(std::size_t cell_count, std::size_t slot_capacity) noexcept
{
    return cell_count * round_up_8(slot_capacity);
}

}

#endif
