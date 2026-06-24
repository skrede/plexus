#ifndef HPP_GUARD_PLEXUS_NATIVE_MACHINE_FINGERPRINT_H
#define HPP_GUARD_PLEXUS_NATIVE_MACHINE_FINGERPRINT_H

#include "plexus/io/host_fingerprint.h"

namespace plexus::native {

// The FNV-1a hash of "machine-id\x1fhostname" (a dependency-free host hash --
// plexus carries no crypto dependency): every process on one host converges on the
// same value, two hosts diverge. When the machine-id is unreadable the hostname
// alone seeds the hash. The repeatability comes from stable inputs, not caching --
// there is no static memoization here.
plexus::io::host_fingerprint read_machine_fingerprint();

}

#endif
