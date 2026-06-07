// The cookie_secret oracle: a pure sans-OpenSSL drive of the source-address cookie rotation +
// mint/validate logic over INJECTED fake hmac_fn/rand_fn (no OpenSSL, no backend link —
// plexus::plexus only; the litmus proof the decision logic needs no crypto lib). It proves:
// mint->validate round-trips against the current nonce; a cookie minted before a rotation still
// validates after one maybe_rotate (the two-nonce straddle); a forged/near-miss cookie fails;
// a degraded rand_fn makes the ctor fail closed AND makes maybe_rotate retain the prior nonces.

#include "plexus/io/security/cookie_secret.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <chrono>
#include <memory>
#include <vector>
#include <cstddef>
#include <utility>

using plexus::io::security::cookie_secret;
using plexus::io::security::hmac_fn;
using plexus::io::security::rand_fn;

namespace {

// A deterministic keyed mix over key||msg, filling all 32 out bytes (a stand-in for HMAC; the
// only property the core logic needs is determinism over its inputs, including the nonce that
// rides in msg). No OpenSSL.
hmac_fn fake_hmac()
{
    return [](std::span<const std::byte> key, std::span<const std::byte> msg, std::span<std::byte> out)
    {
        if(out.size() != 32)
            return false;
        for(std::size_t i = 0; i < out.size(); ++i)
        {
            // A rolling, position-weighted mix (multiply-accumulate, not XOR — XOR over the
            // message can cancel two distinct nonces). Deterministic over key||msg||i.
            unsigned acc = 0x811c9dc5u + static_cast<unsigned>(i);
            for(std::size_t k = 0; k < key.size(); ++k)
                acc = (acc ^ std::to_integer<unsigned>(key[k])) * 0x01000193u + static_cast<unsigned>(k);
            for(std::size_t m = 0; m < msg.size(); ++m)
                acc = (acc ^ std::to_integer<unsigned>(msg[m])) * 0x01000193u + static_cast<unsigned>(m + i);
            out[i] = static_cast<std::byte>(acc & 0xffu);
        }
        return true;
    };
}

// A counter-seeded rand: each fill writes distinct, non-zero, advancing bytes so successive
// fills (key, cur, prev, each rotation) differ. Deterministic for the test.
rand_fn counter_rand(unsigned start = 1)
{
    auto counter = std::make_shared<unsigned>(start);
    return [counter](std::span<std::byte> out)
    {
        for(auto &b : out)
            b = static_cast<std::byte>((*counter)++ & 0xff);
        return true;
    };
}

std::vector<std::byte> addr_of(std::initializer_list<int> vals)
{
    std::vector<std::byte> v;
    for(int x : vals)
        v.push_back(static_cast<std::byte>(x));
    return v;
}

}

TEST_CASE("io.cookie_secret mint then validate round-trips against the current nonce",
          "[io][cookie_secret]")
{
    cookie_secret secret{fake_hmac(), counter_rand()};
    const auto addr = addr_of({127, 0, 0, 1, 0x1f, 0x90});

    std::array<std::byte, cookie_secret::k_cookie_len> cookie{};
    REQUIRE(secret.mint(addr, cookie));
    REQUIRE(secret.validate(addr, cookie));

    // A cookie minted for a different address must not validate.
    const auto other = addr_of({10, 0, 0, 1, 0x1f, 0x90});
    REQUIRE_FALSE(secret.validate(other, cookie));
}

TEST_CASE("io.cookie_secret accepts a cookie minted before one rotation (two-nonce straddle)",
          "[io][cookie_secret]")
{
    cookie_secret secret{fake_hmac(), counter_rand()};
    const auto addr = addr_of({192, 0, 2, 5, 0x04, 0xd2});

    std::array<std::byte, cookie_secret::k_cookie_len> cookie{};
    REQUIRE(secret.mint(addr, cookie));

    // Drive one rotation by jumping past the period: current -> previous. The pre-rotation
    // cookie must still validate against the now-previous nonce.
    auto now = std::chrono::steady_clock::now();
    secret.maybe_rotate(now + cookie_secret::k_default_rotation + std::chrono::seconds{1});
    REQUIRE(secret.validate(addr, cookie));

    // After a SECOND rotation the original cookie's nonce has aged out of the window.
    secret.maybe_rotate(now + 2 * cookie_secret::k_default_rotation + std::chrono::seconds{2});
    REQUIRE_FALSE(secret.validate(addr, cookie));
}

TEST_CASE("io.cookie_secret rejects a forged / near-miss cookie", "[io][cookie_secret]")
{
    cookie_secret secret{fake_hmac(), counter_rand()};
    const auto addr = addr_of({127, 0, 0, 1, 0x1f, 0x90});

    std::array<std::byte, cookie_secret::k_cookie_len> cookie{};
    REQUIRE(secret.mint(addr, cookie));

    // A one-byte-off cookie must fail (the constant-time compare is byte-complete).
    cookie[0] ^= std::byte{0xff};
    REQUIRE_FALSE(secret.validate(addr, cookie));

    // A wrong-length cookie is rejected immediately.
    std::array<std::byte, 8> short_cookie{};
    REQUIRE_FALSE(secret.validate(addr, short_cookie));
}

TEST_CASE("io.cookie_secret fails closed on a degraded rand_fn", "[io][cookie_secret]")
{
    // A rand_fn that always reports failure must abort construction (no zero/constant key).
    rand_fn bad = [](std::span<std::byte>) { return false; };
    REQUIRE_THROWS([&] { cookie_secret secret{fake_hmac(), std::move(bad)}; }());
}

TEST_CASE("io.cookie_secret retains the prior nonces when a rotation's rand_fn fails",
          "[io][cookie_secret]")
{
    // A rand that succeeds for the ctor fills then fails: the rotation must RETAIN the prior
    // good nonces (the window does not advance), so a current cookie still validates.
    auto fills = std::make_shared<int>(0);
    rand_fn flaky = [fills](std::span<std::byte> out)
    {
        if(*fills >= 3) // key + cur + prev succeed; any rotation fill fails
            return false;
        for(auto &b : out)
            b = static_cast<std::byte>((*fills * 7 + 1) & 0xff);
        ++*fills;
        return true;
    };

    cookie_secret secret{fake_hmac(), std::move(flaky)};
    const auto addr = addr_of({203, 0, 113, 9, 0x00, 0x35});

    std::array<std::byte, cookie_secret::k_cookie_len> cookie{};
    REQUIRE(secret.mint(addr, cookie));

    auto now = std::chrono::steady_clock::now();
    secret.maybe_rotate(now + cookie_secret::k_default_rotation + std::chrono::seconds{1});

    // The rand failure retained the prior nonces: the pre-"rotation" cookie still validates.
    REQUIRE(secret.validate(addr, cookie));
}
