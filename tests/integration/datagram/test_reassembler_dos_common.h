#ifndef HPP_GUARD_TESTS_INTEGRATION_REASSEMBLER_DOS_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_REASSEMBLER_DOS_COMMON_H

// The reassembly-DoS bound: the reassembler stays bounded under a forged/partial-fragment
// flood. Each of its four bounds is FORCED, looped, and the in-flight state is
// asserted bounded throughout: the total-memory cap (a new partial that would breach the
// byte cap is rejected), the per-message ceiling (an oversize-claim fragment past
// max_message_size is dropped), malformed rejection (idx>=cnt / cnt==0 / cnt over the
// field-width max — never indexing past the span), AND the per-message timeout eviction
// (the timer path reclaims a stalled partial whose fragments never complete — the path
// most likely to be missed). The cap + timeout defaults are substantiated in reassembler.h
// and exercised here at a compressed config so the test runs fast and deterministically.
//
// Under per-fragment AEAD a forged fragment dies at the tag check in
// datagram_authenticated_channel BEFORE feed runs (proven in test_aead_fragment.cpp), so the
// reassembler rarely sees forged input on the secured path; these bounds are the reassembler's
// OWN defense-in-depth, exercised directly. The demux peer cap is also checked: it bounds the
// spoofed-source channel count, so the per-channel-per-peer reassembler structure bounds the
// aggregate reassembly memory.

#include "plexus/datagram/detail/reassembler.h"
#include "plexus/datagram/detail/inbound_demux.h"
#include "plexus/io/fragmentation.h"

#include "plexus/testing/harness.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <chrono>

using namespace std::chrono_literals;

namespace reassembler_dos_fixture {

using test_reassembler =
        plexus::datagram::detail::reassembler<plexus::inproc::inproc_executor<plexus::testing::test_clock> &, plexus::inproc::inproc_timer<plexus::testing::test_clock>>;

inline std::vector<std::byte> filler(std::size_t n)
{
    std::vector<std::byte> v(n);
    for(std::size_t i = 0; i < n; ++i)
        v[i] = static_cast<std::byte>((i * 17u + 3u) & 0xFFu);
    return v;
}

}

#endif
