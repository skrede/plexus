#include "test_udp_isn_common.h"

using namespace udp_isn_fixture;

TEST_CASE("udp isn: forged fragmented-flagged segments the reorder buffer rejects leave no residue", "[udp][isn][spoof][frag]")
{
    // A forged reliable-data segment carrying the FRAGMENTED envelope bit that the reorder
    // buffer PROVABLY rejects must not deposit any per-seq receive-side state: the fragmented
    // flag rides reorder-buffer ACCEPTANCE, so a rejected segment leaves nothing behind and
    // cannot corrupt a subsequent legitimate delivery on the same channel. The forged seqs
    // are placed strictly BELOW the receiver's negotiated ISN, so they are guaranteed-old
    // duplicates under the RFC-1982 serial arithmetic (adv >= half_space) regardless of the
    // session's random ISN — a deterministically-rejected burst, not one whose rejection
    // depends on a probabilistic ISN/seq-range relationship. The observable proof is
    // end-to-end: after the rejected burst, a normal udpr exchange still round-trips
    // byte-identically and nothing forged is ever delivered.
    //
    // This asserts the TRUE plaintext-path contract: a segment the buffer rejects leaves no
    // residue. Absolute segment authenticity (rejecting a forgery that lands INSIDE the
    // window) is the AEAD path's job; the cleartext udpr path is cleartext-equivalent by
    // design (udp_envelope.h) and its sole off-path defense is the random ISN.
    constexpr int k_iterations = 20;
    int proven                 = 0;
    for(int iter = 0; iter < k_iterations; ++iter)
    {
        ::asio::io_context io;
        pasio::udp_transport server{io, pasio::udp_channel::default_max_payload, pasio::udp_transport::arq_type::default_ladder, fast_arq()};
        pasio::udp_transport client{io, pasio::udp_channel::default_max_payload, fast_hs, fast_arq()};

        std::unique_ptr<pasio::udp_channel> accepted, dialed;
        std::vector<std::string> delivered;
        server.on_accepted(
                [&](std::unique_ptr<pasio::udp_channel> ch)
                {
                    accepted = std::move(ch);
                    accepted->on_data([&](std::span<const std::byte> b) { delivered.push_back(str_of(b)); });
                });
        server.listen({"udp", "127.0.0.1:0"});
        pump_until(io, [&] { return server.port() != 0; });

        client.on_dialed([&](std::unique_ptr<pasio::udp_channel> ch, const pio::endpoint &) { dialed = std::move(ch); });
        client.dial({"udpr", "127.0.0.1:" + std::to_string(server.port())});
        pump_until(io, [&] { return dialed && accepted; });
        REQUIRE(dialed != nullptr);
        REQUIRE(accepted != nullptr);

        // A burst of forged FRAGMENTED-flagged data segments at the 200 seqs immediately
        // BELOW the receiver's negotiated ISN (isn-1, isn-2, ...). Each adv = seq - expected
        // wraps to >= half_space, so the reorder buffer classifies every one as a duplicate
        // and drops it — a provable rejection independent of the random ISN value. Pre-fix
        // these inserted into a per-seq set BEFORE the reorder buffer ever validated the seq;
        // post-fix the bit rides acceptance only, so a rejected seq deposits nothing.
        const std::uint16_t isn = accepted->initial_seq();
        for(std::uint16_t k = 1; k <= 200; ++k)
        {
            const auto s = static_cast<std::uint16_t>(isn - k);
            std::vector<std::byte> inner;
            wire::encode_udp_segment_into(inner, bytes_of("FORGED-FRAG"));
            std::vector<std::byte> spoof;
            wire::wrap_udp_into_fragmented(spoof, wire::udp_envelope_kind::reliable_arq, s, inner);
            accepted->deliver_inbound(spoof);
        }

        std::vector<std::string> sent;
        for(int i = 0; i < 4; ++i)
        {
            const std::string p = "frag-isn-" + std::to_string(iter) + "-" + std::to_string(i);
            sent.push_back(p);
            dialed->send(bytes_of(p));
        }
        pump_until(io, [&] { return delivered.size() == 4; });

        REQUIRE(delivered == sent); // legitimate traffic unharmed; nothing forged delivered
        ++proven;
    }
    REQUIRE(proven == k_iterations);
}
