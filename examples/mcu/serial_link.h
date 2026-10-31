#ifndef HPP_GUARD_PLEXUS_EXAMPLE_SERIAL_LINK_H
#define HPP_GUARD_PLEXUS_EXAMPLE_SERIAL_LINK_H

#include <cstddef>

namespace example {

// Both ends of a serial link run the same payload cap, because a deframer refuses any frame larger than its own.
inline constexpr std::size_t k_gate_link_max_payload  = 256; // dialed small for the constrained target
inline constexpr std::size_t k_bench_link_max_payload = 8 * 1024;

}

#endif
