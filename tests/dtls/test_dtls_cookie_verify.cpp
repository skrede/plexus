#include "test_dtls_cookie_common.h"

TEST_CASE("dtls.cookie: the cookie verify callback rejects a forged cookie, looped",
          "[dtls][cookie]")
{
    // The cookie HMAC binds [peer_addr || nonce] under a process-random key; a cookie
    // the secret did not mint must not validate. mint() with the real addr is the only
    // path that produces a valid cookie; a flipped byte / a foreign cookie is rejected
    // (validate() checks the current AND previous nonce, constant-time).
    auto                       secret = ptls::make_cookie_secret();
    const std::byte            addr[] = {std::byte{127}, std::byte{0},    std::byte{0},
                                         std::byte{1},   std::byte{0x1f}, std::byte{0x90}};
    std::span<const std::byte> addr_span{addr, sizeof(addr)};

    constexpr int k_iterations = 100;
    int           rejected     = 0;
    for(int i = 0; i < k_iterations; ++i)
    {
        std::array<std::byte, plexus::io::security::cookie_secret::k_cookie_len> good{};
        REQUIRE(secret.mint(addr_span, good));
        REQUIRE(secret.validate(addr_span, good)); // the minted cookie validates

        // A forged cookie (the good one with a flipped byte) must not validate.
        auto forged = good;
        forged[0] ^= std::byte{0xff};
        REQUIRE_FALSE(secret.validate(addr_span, forged));

        // A cookie bound to a DIFFERENT peer addr must not validate either.
        const std::byte other_addr[] = {std::byte{127}, std::byte{0},    std::byte{0},
                                        std::byte{1},   std::byte{0x23}, std::byte{0x28}};
        REQUIRE_FALSE(
                secret.validate(std::span<const std::byte>{other_addr, sizeof(other_addr)}, good));
        ++rejected;
    }
    REQUIRE(rejected == k_iterations);
}

TEST_CASE("dtls.failclosed: the ALPN select gate fails closed on a non-overlapping offer, looped",
          "[dtls][failclosed]")
{
    // R-2: the in-handshake ALPN gate. The server's plexus_alpn_select selects
    // "plexus/1" when offered, else returns SSL_TLSEXT_ERR_ALERT_FATAL to FAIL the
    // handshake closed (no silent fallback to an unversioned protocol).
    constexpr int k_iterations = 100;
    int           proven       = 0;
    for(int i = 0; i < k_iterations; ++i)
    {
        const unsigned char *out    = nullptr;
        unsigned char        outlen = 0;

        // A client offering only "h2" (a non-plexus protocol): no overlap -> FATAL.
        const unsigned char offer_other[] = {2, 'h', '2'};
        REQUIRE(ptls::plexus_alpn_select(nullptr, &out, &outlen, offer_other, sizeof(offer_other),
                                         nullptr) == SSL_TLSEXT_ERR_ALERT_FATAL);

        // A client offering "plexus/1": selected -> OK (the positive control).
        const unsigned char  offer_plexus[] = {8, 'p', 'l', 'e', 'x', 'u', 's', '/', '1'};
        const unsigned char *sel            = nullptr;
        unsigned char        sel_len        = 0;
        REQUIRE(ptls::plexus_alpn_select(nullptr, &sel, &sel_len, offer_plexus,
                                         sizeof(offer_plexus), nullptr) == SSL_TLSEXT_ERR_OK);
        REQUIRE(sel_len == 8);
        REQUIRE(std::memcmp(sel, "plexus/1", 8) == 0);
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}
