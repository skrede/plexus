#include "test_nonce_uniqueness_common.h"

using namespace nonce_uniqueness_fixture;

TEST_CASE("crypto.nonce_uniqueness holds across flow, reconnect, and restart (looped)", "[crypto][nonce]")
{
    std::set<nonce_tuple> seen;
    const std::uint64_t rekey_threshold = 64; // exercise several rekeys per session
    const std::uint64_t per_session     = 500;

    // N looped runs of the three-scenario set with a deterministic, RNG-free
    // construction — a second ctest invocation reproduces the identical pass.
    const int loops = 8;
    for(int run = 0; run < loops; ++run)
    {
        const std::uint32_t base = static_cast<std::uint32_t>(run * 10);
        drive_flow(seen, base + 1, per_session, rekey_threshold); // steady-state flow
        drive_flow(seen, base + 2, per_session, rekey_threshold); // reconnect: fresh session/keys
        drive_flow(seen, base + 3, per_session,
                   rekey_threshold); // forced restart: fresh session/keys
    }

    REQUIRE(seen.size() == static_cast<std::size_t>(loops) * 3u * per_session);
}

TEST_CASE("crypto.nonce_uniqueness holds across a > 256-rekey session (epoch byte wrap)", "[crypto][nonce]")
{
    // Force > 256 rekeys in one session (a rekey every frame) so the 8-bit wire epoch
    // byte wraps 0xff -> 0x00. Each epoch installs a FRESH key (a distinct fingerprint),
    // so the (key-fingerprint, epoch, sequence, direction) set stays unique across the
    // wrap — the wire-byte collision (epoch 5 and 261 share byte 5) never reuses a
    // (key,nonce) pair because the keys differ.
    std::set<nonce_tuple> seen;
    const std::uint64_t frames = 600; // well past 256 rekeys at threshold 1
    drive_flow(seen, /*session_salt=*/77, frames, /*rekey_threshold=*/1);
    REQUIRE(seen.size() == static_cast<std::size_t>(frames));

    // Run a second time into a separate set to confirm the construction is reproducible
    // (RNG-free): the identical tuple set is produced.
    std::set<nonce_tuple> seen_again;
    drive_flow(seen_again, /*session_salt=*/77, frames, /*rekey_threshold=*/1);
    REQUIRE(seen_again == seen);
}
