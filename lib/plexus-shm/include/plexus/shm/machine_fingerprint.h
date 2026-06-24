#ifndef HPP_GUARD_PLEXUS_SHM_MACHINE_FINGERPRINT_H
#define HPP_GUARD_PLEXUS_SHM_MACHINE_FINGERPRINT_H

#include "plexus/io/host_fingerprint.h"

namespace plexus::shm {

// Read this machine's identity (the kernel machine-id + hostname) and derive the
// core host_fingerprint VALUE the same-host decision compares. The READ is the
// platform's irreducible mechanism so it lives in the compiled backend; the
// COMPARE stays in the header-only core (io/host_fingerprint.h).
//
// Determinism + host-distinctness are the only properties required: every process
// on one host converges on the same value, two hosts diverge. It is the FNV-1a
// hash of "machine-id\x1fhostname" (the dependency-free substitute for a
// cryptographic host hash -- plexus carries no such dependency). When the
// machine-id is unreadable the hostname alone seeds the hash, which still
// distinguishes hosts by name.
//
// The node calls this ONCE and stores the result in its own owned state, then
// passes it down -- there is NO function-local static memoization here (the
// no-static-singleton discipline). Calling it twice on one host returns the same
// value because the inputs are stable, not because anything is cached.
[[nodiscard]] plexus::io::host_fingerprint read_machine_fingerprint();

}

#endif
