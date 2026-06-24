#ifndef HPP_GUARD_PLEXUS_TESTS_CRYPTO_TEST_AUTHENTICATED_CHANNEL_COMMON_H
#define HPP_GUARD_PLEXUS_TESTS_CRYPTO_TEST_AUTHENTICATED_CHANNEL_COMMON_H

// The authenticated_channel proof: a header-on frame sealed by the send-side
// decorator is delivered as the original plaintext header-on frame by the
// receive-side decorator; a flipped sealed byte fires on_protocol_close (NOT
// on_error) and closes; the send nonce sequence is a monotonic per-direction
// counter (never reset under a live key, never a CSPRNG draw); a rekey at the
// threshold re-derives a fresh key, bumps the key-epoch, and straddles an
// in-flight pre-rekey frame; and the fragment budget subtracts the AEAD tag.

#include "plexus/crypto/authenticated_channel.h"
#include "plexus/crypto/key_schedule.h"
#include "plexus/crypto/aead_cipher.h"

#include "plexus/io/fragmentation.h"
#include "plexus/io/byte_channel.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/frame.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <functional>

namespace authenticated_channel_fixture {

using plexus::crypto::aead_cipher_id;
using plexus::crypto::authenticated_channel;
using plexus::crypto::derived_keys;
using plexus::crypto::derive_keys;

// A test lower byte_channel: send() forwards the bytes through an injected sink
// (set by the test to drive the peer decorator's lower on_data), and on_data is
// the seam the decorator wires to receive bytes.
class wire_lower
{
public:
    void send(std::span<const std::byte> data)
    {
        m_last.assign(data.begin(), data.end());
        if(m_sink)
            m_sink(std::span<const std::byte>{m_last});
    }
    void close()
    {
        m_closed = true;
    }
    plexus::io::endpoint remote_endpoint() const
    {
        return {"wire", ""};
    }
    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)> cb)
    {
        m_on_data = std::move(cb);
    }
    void on_closed(plexus::detail::move_only_function<void()> cb)
    {
        m_on_closed = std::move(cb);
    }
    void on_error(plexus::detail::move_only_function<void(plexus::io::io_error)> cb)
    {
        m_on_error = std::move(cb);
    }
    void on_protocol_close(plexus::detail::move_only_function<void(plexus::wire::close_cause)> cb)
    {
        m_on_protocol_close = std::move(cb);
    }
    std::size_t backpressured() const
    {
        return 0;
    }

    void feed(std::span<const std::byte> bytes)
    {
        if(m_on_data)
            m_on_data(bytes);
    }

    std::function<void(std::span<const std::byte>)> m_sink;
    std::vector<std::byte> m_last;
    bool m_closed{false};
    plexus::detail::move_only_function<void(std::span<const std::byte>)> m_on_data;
    plexus::detail::move_only_function<void()> m_on_closed;
    plexus::detail::move_only_function<void(plexus::io::io_error)> m_on_error;
    plexus::detail::move_only_function<void(plexus::wire::close_cause)> m_on_protocol_close;
};

static_assert(plexus::io::byte_channel<wire_lower>, "wire_lower must satisfy byte_channel for the decorator test");

inline derived_keys fixed_keys()
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

inline derived_keys swapped(const derived_keys &k)
{
    return derived_keys{.k_send = k.k_recv, .k_recv = k.k_send};
}

inline std::vector<std::byte> make_frame(std::uint64_t session_id, std::string_view payload)
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

#endif
