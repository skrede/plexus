#ifndef HPP_GUARD_PLEXUS_MCU_MCU_BYTE_OWNER_H
#define HPP_GUARD_PLEXUS_MCU_MCU_BYTE_OWNER_H

namespace plexus::mcu {

// The constrained-target byte_owner: the lifetime handle the receive seam binds a
// wire_bytes view to. Every other substrate selects std::shared_ptr<const void> — a
// heap allocation with an atomic refcount per received frame. The constrained target
// has neither budget: no heap on the hot path, no atomic refcount on a single-task
// cooperative loop. So this owner is a fixed, trivially-copyable in-place handle —
// the one deliberate divergence from the other substrates.
//
// It is an empty tag here because the substrate moves no real bytes yet (the byte
// stream is transport-free at this stage). The type exists only to make
// wire_bytes<mcu_byte_owner> instantiate — default-constructible and movable, the
// sole requirements wire_bytes places on its owner. The real pool/serial-buffer
// ownership leg (an intrusive arena ticket carrying a liveness obligation) lands when
// the receive path goes live; the tag is shaped not to preclude it.
struct mcu_byte_owner
{
};

}

#endif
