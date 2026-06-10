#ifndef HPP_GUARD_PLEXUS_IO_CONGESTION_H
#define HPP_GUARD_PLEXUS_IO_CONGESTION_H

#include <cstdint>

namespace plexus::io {

// The back-pressure axis: a single-valued choice naming WHAT a publisher does
// when the send path is saturated — `block` back-pressures the publish (refuses
// the new value so a reliable guarantee is never silently violated by an
// overrun — the safe default); `drop_oldest` evicts the OLDEST queued value to
// admit the new one (freshest-wins); `drop_newest` sheds the NEW value at a
// saturated queue (the inherent best_effort behavior). Like reliability this is
// an exclusive choice, not a composable mask, so no bitwise operators.
//
// congestion is LOCAL / off-wire — never serialized — so the numeric values are
// renumbered freely for readability (block first as the safe default). The
// shared-memory ring expresses this same axis through reliability x congestion:
// best_effort overwrite-latest is the drop_oldest analog, reliable+block is the
// gating analog — one vocabulary across transports.
enum class congestion : std::uint8_t
{
    block,
    drop_oldest,
    drop_newest,
};

}

#endif
