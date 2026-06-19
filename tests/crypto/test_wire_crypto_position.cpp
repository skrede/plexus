// The wire-capture crypto-position proof. The DEFAULT captures the CLEARTEXT framed bytes
// ABOVE the AEAD seal (the application/protocol-debuggable plaintext the process already holds
// in clear): a recording_channel over a plaintext lower channel taps the header-on frame
// byte-identical to what was sent (proven live over the plaintext path). The OPT-IN captures
// the CIPHERTEXT BELOW the seal: a recording_channel composed UNDER an authenticated_channel
// taps the sealed `epoch||header||ciphertext||tag` bytes — byte-different from the cleartext,
// matching the auth channel's on-wire layout (proven at the unit level against the auth channel
// directly; the secured node stack is NOT wired here — out of scope).

#include "plexus/crypto/authenticated_channel.h"
#include "plexus/crypto/key_schedule.h"
#include "plexus/crypto/aead_cipher.h"

#include "plexus/io/byte_channel.h"
#include "plexus/io/recording_channel.h"

#include "plexus/wire/frame_codec.h"
#include "plexus/wire/frame.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <string>
#include <vector>
#include <memory>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <functional>

using plexus::crypto::aead_cipher_id;
using plexus::crypto::authenticated_channel;
using plexus::crypto::derived_keys;
using plexus::crypto::derive_keys;
using plexus::io::recording_channel;
using plexus::io::recording::wire_direction;

namespace {

// A plaintext lower byte_channel: send() records the bytes that reached it (what crossed the
// wire below any decorator) and forwards them to an injected sink.
class plaintext_lower
{
public:
    void send(std::span<const std::byte> data)
    {
        m_sent.assign(data.begin(), data.end());
        if(m_sink)
            m_sink(std::span<const std::byte>{m_sent});
    }
    void                               close() {}
    [[nodiscard]] plexus::io::endpoint remote_endpoint() const { return {"wire", ""}; }
    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)> cb)
    {
        m_on_data = std::move(cb);
    }
    void on_closed(plexus::detail::move_only_function<void()> cb) { m_on_closed = std::move(cb); }
    void on_error(plexus::detail::move_only_function<void(plexus::io::io_error)> cb)
    {
        m_on_error = std::move(cb);
    }
    void on_protocol_close(plexus::detail::move_only_function<void(plexus::wire::close_cause)> cb)
    {
        m_on_protocol_close = std::move(cb);
    }
    [[nodiscard]] std::size_t backpressured() const { return 0; }

    std::vector<std::byte>                                               m_sent;
    std::function<void(std::span<const std::byte>)>                      m_sink;
    plexus::detail::move_only_function<void(std::span<const std::byte>)> m_on_data;
    plexus::detail::move_only_function<void()>                           m_on_closed;
    plexus::detail::move_only_function<void(plexus::io::io_error)>       m_on_error;
    plexus::detail::move_only_function<void(plexus::wire::close_cause)>  m_on_protocol_close;
};

static_assert(plexus::io::byte_channel<plaintext_lower>,
              "plaintext_lower must satisfy byte_channel");

derived_keys fixed_keys()
{
    std::vector<std::byte> psk;
    for(char c : std::string{"a-shared-pre-shared-key-of-decent-length"})
        psk.push_back(static_cast<std::byte>(static_cast<unsigned char>(c)));
    std::array<std::byte, 16> in_nonce{};
    std::array<std::byte, 16> rs_nonce{};
    std::array<std::byte, 32> transcript{};
    for(std::size_t i = 0; i < 16; ++i)
    {
        in_nonce[i] = static_cast<std::byte>(0x10 + i);
        rs_nonce[i] = static_cast<std::byte>(0xa0 + i);
    }
    for(std::size_t i = 0; i < 32; ++i)
        transcript[i] = static_cast<std::byte>(0x40 + i);
    derived_keys k{};
    REQUIRE(derive_keys(psk, in_nonce, rs_nonce, transcript, k));
    return k;
}

std::vector<std::byte> make_frame(std::uint64_t session_id, std::string_view payload)
{
    plexus::wire::frame_header hdr{};
    hdr.type         = plexus::wire::msg_type::unidirectional;
    hdr.flags        = 0;
    hdr.session_id   = session_id;
    hdr.timestamp_ns = 123456;
    hdr.payload_len  = payload.size();
    std::vector<std::byte> pt;
    for(char c : payload)
        pt.push_back(static_cast<std::byte>(static_cast<unsigned char>(c)));
    return plexus::wire::encode_frame(hdr, pt);
}

}

TEST_CASE("crypto.wire_crypto_position cleartext-above captures the plaintext frame (live)",
          "[crypto][wire][wire_crypto_position]")
{
    // The decorator sits ABOVE any seal, over a plaintext lower channel: the captured OUT
    // bytes are the application-debuggable plaintext frame_header+payload, byte-identical to
    // what was sent. This is the DEFAULT position, proven live over the plaintext path.
    auto                              *raw = new plaintext_lower;
    recording_channel<plaintext_lower> dec{std::unique_ptr<plaintext_lower>(raw)};

    std::vector<std::byte> captured;
    dec.on_wire(
            [&](wire_direction dir, std::uint64_t, std::span<const std::byte> b)
            {
                if(dir == wire_direction::out)
                    captured.assign(b.begin(), b.end());
            });

    const auto frame = make_frame(7, "application-debuggable plaintext");
    dec.send(std::span<const std::byte>{frame});

    // Captured cleartext == the frame == the bytes the lower channel saw (no seal in the path).
    REQUIRE(captured == frame);
    REQUIRE(raw->m_sent == frame);
}

TEST_CASE("crypto.wire_crypto_position ciphertext-below captures the sealed bytes (unit)",
          "[crypto][wire][wire_crypto_position]")
{
    // The decorator sits BELOW the seal: the authenticated_channel wraps the recording_channel
    // (which owns the plaintext lower), so a send through the auth channel reaches the decorator
    // ALREADY SEALED. The captured OUT bytes are `epoch(1)||header(28)||ciphertext||tag(16)`,
    // byte-different from the cleartext frame. Driven directly against the auth channel — the
    // secured node stack is NOT wired (out of scope).
    const auto keys = fixed_keys();

    auto                              *raw = new plaintext_lower;
    recording_channel<plaintext_lower> dec{std::unique_ptr<plaintext_lower>(raw)};
    authenticated_channel<recording_channel<plaintext_lower>> auth{
            dec, aead_cipher_id::chacha20_poly1305, keys};

    std::vector<std::byte> captured;
    dec.on_wire(
            [&](wire_direction dir, std::uint64_t, std::span<const std::byte> b)
            {
                if(dir == wire_direction::out)
                    captured.assign(b.begin(), b.end());
            });

    const auto frame = make_frame(7, "sealed below the decorator");
    auth.send(std::span<const std::byte>{frame});

    // The captured bytes are the sealed wire form, NOT the cleartext frame.
    REQUIRE_FALSE(captured.empty());
    REQUIRE(captured != frame);
    // The sealed layout: epoch byte (0 at the initial epoch) + the cleartext header (the AEAD
    // AAD, readable) + ciphertext||tag. The total grows by the 1-byte epoch + the 16-byte tag.
    REQUIRE(captured.size() == frame.size() + 1u + plexus::crypto::k_aead_tag_len);
    REQUIRE(static_cast<std::uint8_t>(captured.front()) == 0u);
    // The header rides in clear as the AAD: bytes [1, 1+header_size) equal the frame header.
    const std::span<const std::byte> sealed_header{captured.data() + 1, plexus::wire::header_size};
    const std::span<const std::byte> frame_header{frame.data(), plexus::wire::header_size};
    REQUIRE(std::vector<std::byte>(sealed_header.begin(), sealed_header.end()) ==
            std::vector<std::byte>(frame_header.begin(), frame_header.end()));
    // What the decorator captured is exactly what the plaintext lower received (the on-wire form).
    REQUIRE(raw->m_sent == captured);
}
