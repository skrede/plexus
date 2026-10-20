#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_LIVENESS_TOKEN_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_LIVENESS_TOKEN_H

#include <cstddef>

namespace plexus::io::detail {

// Heap-held teardown-quiescence state shared by an owner and every async operation it issues.
// Closing an I/O object aborts only PENDING operations, so a completion already queued when the
// owner is destroyed still runs; each completion holds a copy of this block, so the reads it makes
// at entry hit owner-independent storage, not the freed owner. The owner's destructor clears
// `alive` before the teardown that aborts pending operations, so a late completion no-ops;
// `outstanding` counts issued-but-uncompleted operations; `closed_fired` makes a posted close
// notification fire exactly once.
struct liveness_token
{
    std::size_t outstanding = 0;
    bool alive              = true;
    bool closed_fired       = false;
};

}

#endif
