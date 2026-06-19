// over-limit: one cohesive AEAD-over-fragment composition matrix; the in-order, reorder, and
// forged-tag cells share the one seal+split+reassembler harness on the inproc test clock, so
// splitting them scatters that shared fixture The per-fragment AEAD composition proof: the
// per-fragment seal composes with the fragmenter (io::split) and the reassembler END TO END, in the
// shape the code already has — fragment FIRST, then seal each fragment as its own datagram
// (per-fragment auth; per-message and hybrid are rejected, never re-layered here). A large (>= 1
// MiB) message fragments into sealed datagrams that round-trip byte-identically through
// datagram_authenticated_channel + reassembler under no loss AND under fragment-scale
// reorder (the deterministic loss/reorder scheduler), the swept anti-replay window
// (k_anti_replay_window_bits) admitting the reordered fragments. A single forged/tampered
// fragment fails open() at the tag check and NEVER reaches the reassembler — the
// structural reassembly-DoS defense, asserted directly (the reassembler observes nothing
// for the forged datagram). The verify-before-commit (probe -> open -> commit) order in
// the decorator is preserved (the nonce/window are untouched here).

#include "plexus/crypto/datagram_authenticated_channel.h"
#include "plexus/crypto/key_schedule.h"
#include "plexus/crypto/aead_cipher.h"

#include "plexus/io/detail/reassembler.h"
#include "plexus/io/fragmentation.h"
#include "plexus/io/byte_channel.h"

#include "plexus/wire/udp_envelope.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/frame.h"

#include "plexus/testing/harness.h"

#include "support/loss_reorder_shim.h"

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

// A capture-and-reinject byte_channel double: send() records the on-wire datagram; feed()
// pushes one datagram up to the decorator's on_data (the receiver path). Mirrors the
// wire_lower double the datagram_replay test uses.
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

// A deterministic large payload: a position-dependent byte pattern so a wrong-order or
// dropped fragment shows as a content mismatch, not a coincidental zero match.
std::vector<std::byte> make_message(std::size_t n)
{
    std::vector<std::byte> v(n);
    for(std::size_t i = 0; i < n; ++i)
        v[i] = static_cast<std::byte>((i * 31u + (i >> 8u) * 7u + 0x5Au) & 0xFFu);
    return v;
}

// Wrap one bare fragment payload ([msg_id][frag_idx][frag_cnt][slice]) in a frame_header
// (the AEAD AAD) so it is a valid header-on frame the decorator seals payload-only.
std::vector<std::byte> frame_for_fragment(std::span<const std::byte> frag_payload)
{
    plexus::wire::frame_header hdr{};
    hdr.type         = plexus::wire::msg_type::unidirectional;
    hdr.flags        = 0;
    hdr.session_id   = 7;
    hdr.timestamp_ns = 7777;
    hdr.payload_len  = frag_payload.size();
    return plexus::wire::encode_frame(hdr, frag_payload);
}

// Fragment + seal: split `message` at the AEAD-decorated budget, seal each fragment as
// its own datagram through a sender datagram_authenticated_channel, and capture the
// sealed datagrams on the wire in emission order.
std::vector<std::vector<std::byte>> fragment_and_seal(const derived_keys        &keys,
                                                      std::span<const std::byte> message,
                                                      std::size_t budget, std::uint16_t msg_id,
                                                      std::uint32_t &frag_cnt_out)
{
    wire_lower                                 send_wire;
    datagram_authenticated_channel<wire_lower> sender(send_wire, aead_cipher_id::chacha20_poly1305,
                                                      keys);

    std::vector<std::vector<std::byte>> on_wire;
    send_wire.m_sink = [&](std::span<const std::byte> b)
    { on_wire.emplace_back(b.begin(), b.end()); };

    std::vector<std::byte>    frag_scratch;
    plexus::io::fragment_sink sink =
            [&](std::uint32_t idx, std::uint32_t cnt, std::span<const std::byte> slice)
    {
        plexus::wire::encode_udp_fragment_payload_into(frag_scratch, msg_id, idx, cnt, slice);
        sender.send(frame_for_fragment(frag_scratch));
    };

    frag_cnt_out = plexus::io::split(message, budget, msg_id, sink, /*aead_decorated=*/true);
    return on_wire;
}

using test_reassembler = plexus::io::detail::reassembler<
        plexus::inproc::inproc_executor<plexus::testing::test_clock> &,
        plexus::inproc::inproc_timer<plexus::testing::test_clock>>;

// The receiver half: a datagram_authenticated_channel whose opened frames are stripped of
// their header, fragment-decoded, and fed to the reassembler. Records every fragment the
// reassembler actually sees (the witness — a forged datagram must add nothing here).
struct receiver_pipe
{
    wire_lower                                 recv_wire;
    datagram_authenticated_channel<wire_lower> channel;
    test_reassembler                           reasm;
    std::size_t                                fragments_fed{0};

    receiver_pipe(const derived_keys &keys, plexus::testing::harness &h)
            : channel(recv_wire, aead_cipher_id::chacha20_poly1305, swapped(keys))
            , reasm(h.ex)
    {
        channel.on_data(
                [this](std::span<const std::byte> frame)
                {
                    const auto payload = frame.subspan(plexus::wire::header_size);
                    const auto frag    = plexus::wire::decode_udp_fragment_header(payload);
                    if(!frag)
                        return;
                    ++fragments_fed;
                    reasm.feed(frag->msg_id, frag->frag_idx, frag->frag_cnt, frag->payload);
                });
    }

    void deliver(std::span<const std::byte> sealed_datagram) { recv_wire.feed(sealed_datagram); }
};

}

TEST_CASE("integration.aead_fragment a sealed-fragment large message round-trips in order",
          "[aead][fragment][dgram]")
{
    const auto        keys    = fixed_keys();
    const auto        message = make_message(1u * 1024u * 1024u); // 1 MiB
    const std::size_t budget  = 1200;

    for(int run = 0; run < 3; ++run) // a transport claim is never made from one run
    {
        std::uint32_t frag_cnt = 0;
        const auto    wire = fragment_and_seal(keys, message, budget, /*msg_id=*/0x2A, frag_cnt);
        REQUIRE(frag_cnt > 1);
        REQUIRE(wire.size() == frag_cnt);

        plexus::testing::harness h;
        receiver_pipe            rx(keys, h);

        std::vector<std::byte> delivered;
        rx.reasm.on_deliver([&](std::span<const std::byte> b)
                            { delivered.assign(b.begin(), b.end()); });

        for(const auto &dg : wire)
            rx.deliver(dg);

        REQUIRE(delivered == message);
        REQUIRE(rx.channel.dropped_count() == 0);
        REQUIRE(rx.fragments_fed == frag_cnt);
    }
}

TEST_CASE("integration.aead_fragment a sealed-fragment large message round-trips under "
          "fragment-scale reorder",
          "[aead][fragment][dgram][reorder]")
{
    const auto keys    = fixed_keys();
    const auto message = make_message(1u * 1024u * 1024u); // 1 MiB -> ~900 fragments at 1200 B
    const std::size_t budget = 1200;

    for(int run = 0; run < 3; ++run)
    {
        std::uint32_t frag_cnt = 0;
        const auto    wire = fragment_and_seal(keys, message, budget, /*msg_id=*/0x2A, frag_cnt);

        // Reorder the sealed datagrams at fragment scale through the deterministic
        // scheduler: a bounded reorder window well inside the swept anti-replay width, so
        // every reordered-but-fresh fragment opens and the message still completes. The
        // schedule is byte-identical across runs (RNG-free LCG).
        plexus::testing::loss_reorder_scheduler sched(plexus::testing::loss_reorder_config{
                .loss_num = 0, .reorder_depth = 64, .seed = 0x5eed1234abcd0011ull});
        std::vector<std::vector<std::byte>>     reordered;
        for(const auto &dg : wire)
            for(auto &out : sched.drive(std::span<const std::byte>{dg}))
                reordered.push_back(std::move(out));
        for(auto &out : sched.flush())
            reordered.push_back(std::move(out));
        REQUIRE(reordered.size() == wire.size());

        plexus::testing::harness h;
        receiver_pipe            rx(keys, h);

        std::vector<std::byte> delivered;
        rx.reasm.on_deliver([&](std::span<const std::byte> b)
                            { delivered.assign(b.begin(), b.end()); });

        for(const auto &dg : reordered)
            rx.deliver(dg);

        REQUIRE(delivered == message);           // byte-identical despite reorder
        REQUIRE(rx.channel.replay_count() == 0); // the swept window admitted every fragment
        REQUIRE(rx.fragments_fed == frag_cnt);
    }
}

TEST_CASE("integration.aead_fragment a forged fragment dies at the tag check before reassembly",
          "[aead][fragment][dgram][forged]")
{
    const auto        keys    = fixed_keys();
    const auto        message = make_message(64u * 1024u); // a few dozen fragments
    const std::size_t budget  = 1200;

    for(int run = 0; run < 3; ++run)
    {
        std::uint32_t frag_cnt = 0;
        auto          wire = fragment_and_seal(keys, message, budget, /*msg_id=*/0x2A, frag_cnt);
        REQUIRE(frag_cnt > 2);

        plexus::testing::harness h;
        receiver_pipe            rx(keys, h);

        bool delivered = false;
        rx.reasm.on_deliver([&](std::span<const std::byte>) { delivered = true; });

        // Tamper with one fragment's sealed ciphertext+tag region. It fails open() at the
        // tag check under per-fragment auth; the reassembler must NEVER see it (no feed).
        const std::size_t victim = frag_cnt / 2;
        auto              forged = wire[victim];
        forged.back() ^= std::byte{0xff};

        rx.deliver(std::span<const std::byte>{forged});
        REQUIRE(rx.channel.tamper_dropped_count() == 1);
        REQUIRE(rx.fragments_fed == 0);     // the forged fragment never reached the reassembler
        REQUIRE(rx.reasm.in_flight() == 0); // no reassembly state minted by the forgery
        REQUIRE_FALSE(delivered);

        // The honest fragments still reassemble around the dropped forgery is NOT asserted
        // (a real best-effort drop of one fragment loses the whole message — the recorded
        // drop-whole semantics); the point here is the structural defense, not recovery.
    }
}

TEST_CASE("integration.aead_fragment the AEAD-decorated budget leaves room for the seal overhead",
          "[aead][fragment][dgram][budget]")
{
    const std::size_t budget = 1200;
    // effective_fragment_budget(.., aead_decorated=true) subtracts the per-fragment seal
    // overhead so a sealed fragment still fits the transport budget.
    const auto eff_aead  = plexus::io::effective_fragment_budget(budget, /*aead_decorated=*/true);
    const auto eff_plain = plexus::io::effective_fragment_budget(budget, /*aead_decorated=*/false);
    REQUIRE(eff_aead + plexus::io::k_aead_fragment_overhead == eff_plain);

    // The fragment SLICE the splitter emits respects the AEAD-decorated budget so a
    // fragment sealed as the udp sub-header(10) + slice + the 25-byte seal overhead fits
    // the transport budget. This harness additionally wraps each fragment payload in a
    // frame_header (the AEAD AAD), so the on-wire sealed datagram carries that fixed
    // header_size too — the production datagram fragment path keys the budget off the udp
    // overhead, not a stream frame_header, so the meaningful ceiling for THIS pipe is the
    // transport budget plus the harness's header framing.
    const auto    keys     = fixed_keys();
    const auto    message  = make_message(eff_aead * 3 + 17);
    std::uint32_t frag_cnt = 0;
    const auto    wire     = fragment_and_seal(keys, message, budget, /*msg_id=*/1, frag_cnt);
    REQUIRE(frag_cnt >= 2);
    for(const auto &dg : wire)
    {
        // The sealed slice (datagram minus seq+epoch+header+tag) stays at or below the
        // AEAD-decorated effective budget plus the udp fragment sub-header it carries.
        const std::size_t sealed_overhead = 8u + 1u + plexus::wire::header_size + 16u;
        REQUIRE(dg.size() > sealed_overhead);
        const std::size_t slice = dg.size() - sealed_overhead;
        CHECK(slice <= eff_aead + plexus::wire::udp_fragment_subheader);
    }
}
