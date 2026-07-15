// libFuzzer harness anchor for the key-pattern matcher. The matcher value type and its
// canonical-scan / set-relation invariants arrive in a later wave; this stub compiles and links
// against plexus::core so the harness, its build wiring, and its seed corpus exist before the
// first line of matching code. No global operator-new trap is installed: libFuzzer and its
// instrumentation allocate, so an unconditional trap would abort the fuzzer -- zero-alloc on the
// match path is proven separately via the counting alloc_counter idiom in the unit tests.

#include "plexus/match/key_pattern_bounds.h"
#include "plexus/match/key_pattern_error.h"

#include <cstddef>
#include <cstdint>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *Data, size_t Size)
{
    (void)Data;
    (void)Size;
    return 0;
}
