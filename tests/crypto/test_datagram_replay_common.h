#ifndef HPP_GUARD_PLEXUS_TESTS_CRYPTO_TEST_DATAGRAM_REPLAY_COMMON_H
#define HPP_GUARD_PLEXUS_TESTS_CRYPTO_TEST_DATAGRAM_REPLAY_COMMON_H

// The datagram_authenticated_channel proof: an out-of-order but fresh datagram
// within the window opens and is delivered (the explicit sequence reconstructs the
// nonce regardless of arrival order); a replayed datagram is dropped, delivers
// nothing, and increments the replay counter with no event fired; a bad-tag datagram
// is dropped, delivers nothing, increments the tamper counter, and does NOT fire
// on_protocol_close; a datagram displaced below the window floor is dropped as
// too-old. The drop counters read on demand.

#include "plexus/crypto/datagram_authenticated_channel.h"
#include "plexus/crypto/anti_replay_window.h"
#include "plexus/crypto/key_schedule.h"
#include "plexus/crypto/aead_cipher.h"

#include "plexus/io/byte_channel.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/frame.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <limits>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <functional>

namespace datagram_replay_fixture {

using plexus::crypto::aead_cipher_id;
using plexus::crypto::datagram_authenticated_channel;
using plexus::crypto::derived_keys;
using plexus::crypto::derive_keys;

class wire_lower
{
public:
    void send(std::span<const std::byte> data)
    {
        m_last.assign(data.begin(), data.end());
        if(m_sink)
            m_sink(std::span<const std::byte>{m_last});
    }
    void                               close() { m_closed = true; }
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

    void feed(std::span<const std::byte> bytes)
    {
        if(m_on_data)
            m_on_data(bytes);
    }

    std::function<void(std::span<const std::byte>)>                      m_sink;
    std::vector<std::byte>                                               m_last;
    bool                                                                 m_closed{false};
    plexus::detail::move_only_function<void(std::span<const std::byte>)> m_on_data;
    plexus::detail::move_only_function<void()>                           m_on_closed;
    plexus::detail::move_only_function<void(plexus::io::io_error)>       m_on_error;
    plexus::detail::move_only_function<void(plexus::wire::close_cause)>  m_on_protocol_close;
};

static_assert(plexus::io::byte_channel<wire_lower>,
              "wire_lower must satisfy byte_channel for the decorator test");

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
    hdr.timestamp_ns = 7777;
    hdr.payload_len  = payload.size();
    std::vector<std::byte> pt;
    for(char c : payload)
        pt.push_back(static_cast<std::byte>(static_cast<unsigned char>(c)));
    return plexus::wire::encode_frame(hdr, pt);
}

// Seal a sequence of datagrams the sender emits, capturing each on the wire so the
// test can reorder/replay them into the receiver at will.
inline std::vector<std::vector<std::byte>> seal_datagrams(const derived_keys &keys,
                                                          std::size_t         count)
{
    wire_lower                                 send_wire;
    datagram_authenticated_channel<wire_lower> sender(send_wire, aead_cipher_id::chacha20_poly1305,
                                                      keys);
    std::vector<std::vector<std::byte>>        on_wire;
    send_wire.m_sink = [&](std::span<const std::byte> b)
    { on_wire.emplace_back(b.begin(), b.end()); };
    for(std::size_t i = 0; i < count; ++i)
        sender.send(make_frame(7, "datagram-payload-" + std::to_string(i)));
    return on_wire;
}

// The decorator's one deterministic rekey step (mirrors its private derive_forward):
// re-derive from the retiring key as IKM under a zeroed nonce/transcript.
inline plexus::crypto::aead_key step_forward(const plexus::crypto::aead_key &from)
{
    std::array<std::byte, 16> nonce{};
    std::array<std::byte, 32> transcript{};
    derived_keys              d{};
    REQUIRE(derive_keys(std::span<const std::byte>{from}, nonce, nonce, transcript, d));
    return d.k_send;
}

// Overwrite the explicit 8-byte BE sequence field of an on-wire datagram in place.
inline void set_wire_seq(std::vector<std::byte> &dg, std::uint64_t seq)
{
    for(std::size_t i = 0; i < 8; ++i)
        dg[i] = static_cast<std::byte>((seq >> (8u * (7u - i))) & 0xffu);
}

}

#endif
