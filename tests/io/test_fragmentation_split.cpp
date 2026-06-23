#include "test_fragmentation_common.h"

using namespace fragmentation_fixture;

namespace {

struct recorded_fragment
{
    std::uint32_t          idx;
    std::uint32_t          cnt;
    std::vector<std::byte> bytes;
};

std::vector<recorded_fragment> collect(std::span<const std::byte> payload, std::size_t budget,
                                       bool aead_decorated = false)
{
    std::vector<recorded_fragment> seen;
    io::fragment_sink              sink =
            [&seen](std::uint32_t idx, std::uint32_t cnt, std::span<const std::byte> b)
    { seen.push_back({idx, cnt, std::vector<std::byte>(b.begin(), b.end())}); };
    const auto returned = io::split(payload, budget, /*msg_id*/ 1, sink, aead_decorated);
    REQUIRE(returned == seen.size());
    return seen;
}

}

TEST_CASE("the splitter fragments against the default budget and honors an upward override",
          "[fragment][split][mtu]")
{
    // The splitter consults the mtu_budget the channel passes: at the conservative 1200-byte
    // default a payload above it fragments, and an explicit higher budget (the tunable knob)
    // yields fewer, larger fragments — the per-datagram budget is configurable up, never
    // silently clamped to the default.
    const std::vector<std::byte> payload(datagram::mtu_budget{}.max_payload * 4);

    const auto at_default = collect(payload, datagram::mtu_budget{}.max_payload);
    const auto at_override =
            collect(payload, datagram::mtu_budget{.max_payload = 8192}.max_payload);

    REQUIRE(at_default.size() > 1);                // genuinely fragmented at 1200
    CHECK(at_override.size() < at_default.size()); // the larger budget fragments less
    CHECK(at_override.front().bytes.size() > at_default.front().bytes.size());
}

TEST_CASE("split against a budget yields one fragment when the payload fits", "[fragment][split]")
{
    const std::vector<std::byte> payload(100);
    auto                         frags = collect(payload, /*budget*/ 1400);
    REQUIRE(frags.size() == 1);
    CHECK(frags[0].idx == 0);
    CHECK(frags[0].cnt == 1);
    CHECK(frags[0].bytes.size() == 100);
}

TEST_CASE("split a payload just over the effective budget yields two fragments",
          "[fragment][split]")
{
    const std::size_t            budget = 100;
    const std::size_t            eff    = io::effective_fragment_budget(budget);
    const std::vector<std::byte> payload(eff + 1);

    auto frags = collect(payload, budget);
    REQUIRE(frags.size() == 2);
    CHECK(frags[0].cnt == 2);
    CHECK(frags[0].bytes.size() == eff);
    CHECK(frags[1].idx == 1);
    CHECK(frags[1].bytes.size() == 1);
}

TEST_CASE("split a large payload yields N fragments, N-1 full-sized in index order",
          "[fragment][split]")
{
    const std::size_t            budget = 200;
    const std::size_t            eff    = io::effective_fragment_budget(budget);
    const std::vector<std::byte> payload(eff * 4 + 7);

    auto frags = collect(payload, budget);
    REQUIRE(frags.size() == 5);

    std::size_t reassembled = 0;
    for(std::size_t i = 0; i < frags.size(); ++i)
    {
        CHECK(frags[i].idx == i); // ascending index order
        CHECK(frags[i].cnt == 5);
        if(i + 1 < frags.size())
            CHECK(frags[i].bytes.size() == eff); // N-1 full-sized
        reassembled += frags[i].bytes.size();
    }
    CHECK(frags.back().bytes.size() == 7); // the remainder
    CHECK(reassembled == payload.size());
}

TEST_CASE("an AEAD-decorated split leaves room for the per-fragment seal overhead so a sealed "
          "fragment fits the MTU",
          "[fragment][split][aead]")
{
    const std::size_t budget = 1400;
    // A payload large enough to force several full-budget fragments on each path.
    const std::vector<std::byte> payload(io::effective_fragment_budget(budget) * 3 + 13);

    const auto aead = collect(payload, budget, /*aead_decorated=*/true);
    REQUIRE(aead.size() >= 2);
    // Every emitted fragment, once the datagram decorator prepends seq+epoch and appends
    // the tag, stays inside the transport budget — the previously-overrunning case fits.
    for(const auto &f : aead)
        CHECK(f.bytes.size() + io::k_aead_fragment_overhead <= budget);

    // The AEAD budget is strictly tighter than the plaintext budget, so a sealed fragment
    // sized to the plaintext budget would have overrun the MTU by exactly the seal overhead.
    CHECK(io::effective_fragment_budget(budget, /*aead_decorated=*/true) +
                  io::k_aead_fragment_overhead ==
          io::effective_fragment_budget(budget, /*aead_decorated=*/false));
}

TEST_CASE("a non-AEAD split is byte-identical to the default path", "[fragment][split][aead]")
{
    const std::size_t            budget = 512;
    const std::vector<std::byte> payload(io::effective_fragment_budget(budget) * 2 + 5);

    const auto plain = collect(payload, budget, /*aead_decorated=*/false);
    const auto deflt = collect(payload, budget);
    REQUIRE(plain.size() == deflt.size());
    for(std::size_t i = 0; i < plain.size(); ++i)
    {
        CHECK(plain[i].idx == deflt[i].idx);
        CHECK(plain[i].cnt == deflt[i].cnt);
        CHECK(plain[i].bytes == deflt[i].bytes);
    }
}
