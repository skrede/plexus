#ifndef HPP_GUARD_PLEXUS_SHM_RING_CELLS_H
#define HPP_GUARD_PLEXUS_SHM_RING_CELLS_H

#include "plexus/shm/ring_header.h"

#include <atomic>
#include <cstdint>
#include <type_traits>

namespace plexus::shm {

// payload_len sentinel for the SKIPPED tombstone state (see cell_t). A best-effort
// producer that must donate its position past a pinned cell advances that cell's
// sequence to (pos + 1) for Vyukov recognition but stamps this length so the
// consumer recognizes a non-deliverable tombstone and steps its cursor forward
// without delivering. A real message length is always <= slot_capacity, far below
// this value, so the two never collide.
inline constexpr std::uint32_t k_skip_len = 0xFFFF'FFFFu;

#if defined(_MSC_VER)
    #pragma warning(push)
    // The alignas on these cross-process control structs is deliberate cache-line isolation;
    // the padding MSVC warns about under /W4 is the intended layout, not an accident.
    #pragma warning(disable : 4324)
#endif

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

#if defined(_MSC_VER)
    #pragma warning(pop)
#endif

static_assert(std::is_standard_layout_v<cell_t>, "cell_t must be standard-layout for SHM placement");

}

#endif
