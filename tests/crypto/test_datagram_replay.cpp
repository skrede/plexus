// The datagram_authenticated_channel proof: an out-of-order but fresh datagram
// within the window opens and is delivered (the explicit sequence reconstructs the
// nonce regardless of arrival order); a replayed datagram is dropped, delivers
// nothing, and increments the replay counter with no event fired; a bad-tag datagram
// is dropped, delivers nothing, increments the tamper counter, and does NOT fire
// on_protocol_close; a datagram displaced below the window floor is dropped as
// too-old. The drop counters read on demand.

#include "plexus/crypto/datagram_authenticated_channel.h"
#include "plexus/crypto/key_schedule.h"
#include "plexus/crypto/aead_cipher.h"

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

using plexus::crypto::aead_cipher_id;
using plexus::crypto::datagram_authenticated_channel;
using plexus::crypto::derived_keys;
using plexus::crypto::derive_keys;

namespace {

class wire_lower
{
public:
    void send(std::span<const std::byte> data)
    {
        m_last.assign(data.begin(), data.end());
        if(m_sink)
            m_sink(std::span<const std::byte>{m_last});
    }
    void close() { m_closed = true; }
    [[nodiscard]] plexus::io::endpoint remote_endpoint() const { return {"wire", ""}; }
    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)> cb) { m_on_data = std::move(cb); }
    void on_closed(plexus::detail::move_only_function<void()> cb) { m_on_closed = std::move(cb); }
    void on_error(plexus::detail::move_only_function<void(plexus::io::io_error)> cb) { m_on_error = std::move(cb); }
    void on_protocol_close(plexus::detail::move_only_function<void(plexus::wire::close_cause)> cb) { m_on_protocol_close = std::move(cb); }
    [[nodiscard]] std::size_t backpressured() const { return 0; }

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

static_assert(plexus::io::byte_channel<wire_lower>,
    "wire_lower must satisfy byte_channel for the decorator test");

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

derived_keys swapped(const derived_keys &k)
{
    return derived_keys{.k_send = k.k_recv, .k_recv = k.k_send};
}

std::vector<std::byte> make_frame(std::uint64_t session_id, std::string_view payload)
{
    plexus::wire::frame_header hdr{};
    hdr.type = plexus::wire::msg_type::unidirectional;
    hdr.flags = 0;
    hdr.session_id = session_id;
    hdr.timestamp_ns = 7777;
    hdr.payload_len = payload.size();
    std::vector<std::byte> pt;
    for(char c : payload)
        pt.push_back(static_cast<std::byte>(static_cast<unsigned char>(c)));
    return plexus::wire::encode_frame(hdr, pt);
}

// Seal a sequence of datagrams the sender emits, capturing each on the wire so the
// test can reorder/replay them into the receiver at will.
std::vector<std::vector<std::byte>> seal_datagrams(const derived_keys &keys, std::size_t count)
{
    wire_lower send_wire;
    datagram_authenticated_channel<wire_lower> sender(send_wire, aead_cipher_id::chacha20_poly1305, keys);
    std::vector<std::vector<std::byte>> on_wire;
    send_wire.m_sink = [&](std::span<const std::byte> b) { on_wire.emplace_back(b.begin(), b.end()); };
    for(std::size_t i = 0; i < count; ++i)
        sender.send(make_frame(7, "datagram-payload-" + std::to_string(i)));
    return on_wire;
}

// The decorator's one deterministic rekey step (mirrors its private derive_forward):
// re-derive from the retiring key as IKM under a zeroed nonce/transcript.
plexus::crypto::aead_key step_forward(const plexus::crypto::aead_key &from)
{
    std::array<std::byte, 16> nonce{};
    std::array<std::byte, 32> transcript{};
    derived_keys d{};
    REQUIRE(derive_keys(std::span<const std::byte>{from}, nonce, nonce, transcript, d));
    return d.k_send;
}

}

TEST_CASE("crypto.datagram_replay delivers an out-of-order but fresh datagram", "[crypto][datagram_replay]")
{
    const auto keys = fixed_keys();
    const auto wire = seal_datagrams(keys, 5);

    wire_lower recv_wire;
    datagram_authenticated_channel<wire_lower> receiver(recv_wire, aead_cipher_id::chacha20_poly1305, swapped(keys));

    std::vector<std::vector<std::byte>> delivered;
    receiver.on_data([&](std::span<const std::byte> f) { delivered.emplace_back(f.begin(), f.end()); });

    // Reordered arrival: 2, 0, 4, 1, 3 — every fresh datagram opens within the window.
    for(std::size_t i : {std::size_t{2}, std::size_t{0}, std::size_t{4}, std::size_t{1}, std::size_t{3}})
        recv_wire.feed(std::span<const std::byte>{wire[i]});

    REQUIRE(delivered.size() == 5);
    REQUIRE(receiver.dropped_count() == 0);
    REQUIRE(delivered[0] == make_frame(7, "datagram-payload-2"));
    REQUIRE(delivered[3] == make_frame(7, "datagram-payload-1"));
}

TEST_CASE("crypto.datagram_replay drops and counts a replayed datagram with no event", "[crypto][datagram_replay]")
{
    const auto keys = fixed_keys();
    const auto wire = seal_datagrams(keys, 3);

    wire_lower recv_wire;
    datagram_authenticated_channel<wire_lower> receiver(recv_wire, aead_cipher_id::chacha20_poly1305, swapped(keys));

    bool protocol_closed = false;
    receiver.on_protocol_close([&](plexus::wire::close_cause) { protocol_closed = true; });

    int delivered = 0;
    receiver.on_data([&](std::span<const std::byte>) { ++delivered; });

    recv_wire.feed(std::span<const std::byte>{wire[0]});
    recv_wire.feed(std::span<const std::byte>{wire[1]});
    recv_wire.feed(std::span<const std::byte>{wire[0]});   // replay of seq 0
    recv_wire.feed(std::span<const std::byte>{wire[1]});   // replay of seq 1

    REQUIRE(delivered == 2);
    REQUIRE(receiver.replay_count() == 2);
    REQUIRE(receiver.tamper_dropped_count() == 0);
    REQUIRE_FALSE(protocol_closed);
    REQUIRE_FALSE(recv_wire.m_closed);
}

TEST_CASE("crypto.datagram_replay drops and counts a bad-tag datagram without teardown", "[crypto][datagram_replay]")
{
    const auto keys = fixed_keys();
    auto wire = seal_datagrams(keys, 2);

    wire_lower recv_wire;
    datagram_authenticated_channel<wire_lower> receiver(recv_wire, aead_cipher_id::chacha20_poly1305, swapped(keys));

    bool protocol_closed = false;
    bool errored = false;
    receiver.on_protocol_close([&](plexus::wire::close_cause) { protocol_closed = true; });
    recv_wire.on_error([&](plexus::io::io_error) { errored = true; });

    int delivered = 0;
    receiver.on_data([&](std::span<const std::byte>) { ++delivered; });

    // Flip a byte in the sealed ciphertext+tag region of the first datagram.
    wire[0].back() ^= std::byte{0xff};
    recv_wire.feed(std::span<const std::byte>{wire[0]});
    recv_wire.feed(std::span<const std::byte>{wire[1]});   // the honest datagram still opens

    REQUIRE(delivered == 1);
    REQUIRE(receiver.tamper_dropped_count() == 1);
    REQUIRE(receiver.replay_count() == 0);
    REQUIRE_FALSE(protocol_closed);
    REQUIRE_FALSE(recv_wire.m_closed);
}

TEST_CASE("crypto.datagram_replay round-trips across the 256-epoch boundary", "[crypto][datagram_replay]")
{
    const auto keys = fixed_keys();

    wire_lower recv_wire;
    datagram_authenticated_channel<wire_lower> receiver(recv_wire, aead_cipher_id::chacha20_poly1305, swapped(keys));

    std::vector<std::byte> delivered;
    receiver.on_data([&](std::span<const std::byte> f) { delivered.assign(f.begin(), f.end()); });

    // The datagram decorator advances the recv epoch one step per new wire epoch byte
    // (forward-deriving the key). Drive a sender forward through > 256 epochs — each
    // epoch sealed with the matching forward-derived key at initial_epoch = e — so the
    // wire epoch byte wraps 0xff -> 0x00 and the seal/open nonce epoch field must agree.
    plexus::crypto::aead_key send_key = keys.k_send;   // epoch 0 send key
    const std::uint32_t epochs = 600;
    for(std::uint32_t e = 0; e < epochs; ++e)
    {
        wire_lower send_wire;
        derived_keys ek{.k_send = send_key, .k_recv = keys.k_recv};
        datagram_authenticated_channel<wire_lower> sender(send_wire, aead_cipher_id::chacha20_poly1305, ek, e);

        std::vector<std::byte> on_wire;
        send_wire.m_sink = [&](std::span<const std::byte> b) { on_wire.assign(b.begin(), b.end()); };
        const auto frame = make_frame(7, "datagram-epoch-" + std::to_string(e));
        sender.send(frame);

        delivered.clear();
        recv_wire.feed(std::span<const std::byte>{on_wire});
        REQUIRE(delivered == frame);   // opens at every epoch, including across the 0xff->0x00 wrap

        send_key = step_forward(send_key);
    }
    REQUIRE(receiver.tamper_dropped_count() == 0);
    REQUIRE(receiver.replay_count() == 0);
}

TEST_CASE("crypto.datagram_replay drops a datagram below the window floor as too-old", "[crypto][datagram_replay]")
{
    const auto keys = fixed_keys();
    const std::size_t past_window = plexus::crypto::k_anti_replay_window_bits + 8;
    const auto wire = seal_datagrams(keys, past_window + 1);

    wire_lower recv_wire;
    datagram_authenticated_channel<wire_lower> receiver(recv_wire, aead_cipher_id::chacha20_poly1305, swapped(keys));

    int delivered = 0;
    receiver.on_data([&](std::span<const std::byte>) { ++delivered; });

    // Accept the highest sequence first, then feed a far-older one (below the floor).
    recv_wire.feed(std::span<const std::byte>{wire[past_window]});
    recv_wire.feed(std::span<const std::byte>{wire[0]});

    REQUIRE(delivered == 1);
    REQUIRE(receiver.replay_count() == 1);
}
