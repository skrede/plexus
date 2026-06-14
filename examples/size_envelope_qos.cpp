// The single-message size envelope, end to end. A publisher node and a subscriber
// node share one io_context and one static-discovery table over TCP loopback, so the
// example is self-contained and self-verifying — no second shell, no mDNS.
//
// It documents the operator knobs that govern how large a single message may be:
//
//   1. the node-level default ceiling — the transport ctor's global_default, set here to
//      16 MiB so a 16 MB single message clears the per-MESSAGE receive ceiling on BOTH
//      ends (the shipped default is the smaller global_default_max_message_bytes = 8 MiB,
//      printed for reference);
//   2. the always-on aggregate reassembly-memory budget — the transport ctor's
//      reassembly_memory_budget, raised above the 16 MiB default so the 16 MB message
//      also clears the connection-shared backstop that bounds attacker-controlled
//      reassembly memory (the budget gates admit independently of any single message's
//      declared size, so it is the real bound);
//   3. the per-topic override field — topic_qos.max_message_bytes — and the pure helper
//      io::effective_max(topic, node_default) that resolves a topic's ceiling: its own
//      override when set, else the node default.
//
// A 16 MB message round-trips byte-for-byte on the raised ceiling; a message past the
// ceiling is refused by the receiver's reassembler and never delivered.

#include "plexus/node.h"
#include "plexus/publisher.h"
#include "plexus/subscriber.h"
#include "plexus/node_options.h"

#include "plexus/io/fragmentation.h"
#include "plexus/io/message_info.h"

#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_transport.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/wire/stream_inbound.h"

#include <asio/io_context.hpp>

#include <span>
#include <chrono>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <iostream>

namespace pasio = plexus::asio;
namespace pio = plexus::io;
namespace wire = plexus::wire;

namespace {

using node_t = plexus::node<pasio::asio_policy, pasio::asio_transport>;

// A deterministic position-dependent payload, regenerated to check the round-trip is
// byte-identical (a size match alone would not catch a reorder or a corrupt fragment).
std::vector<std::byte> make_payload(std::size_t n)
{
    std::vector<std::byte> out(n);
    for(std::size_t i = 0; i < n; ++i)
        out[i] = static_cast<std::byte>((i * 31u + (i >> 8)) & 0xFFu);
    return out;
}

bool equal_bytes(std::span<const std::byte> a, std::span<const std::byte> b)
{
    return a.size() == b.size() && std::equal(a.begin(), a.end(), b.begin());
}

plexus::node_options node_opts(const char *name, bool eager, std::uint64_t seed)
{
    plexus::node_options opts;
    opts.name = name;
    opts.dial_eagerly = eager;        // one eager dialer converges the loopback link
    opts.redial_seed = seed;
    return opts;
}

template <typename Pred>
void pump_until(::asio::io_context &io, Pred pred,
                std::chrono::seconds bound = std::chrono::seconds{20})
{
    auto deadline = std::chrono::steady_clock::now() + bound;
    while(!pred() && std::chrono::steady_clock::now() < deadline)
    {
        io.poll();
        if(io.stopped())
            io.restart();
    }
}

}

int main()
{
    ::asio::io_context io;

    // The two operator-facing size knobs, raised for the 16 MB path. global_default is the
    // node default per-MESSAGE ceiling stamped onto every minted channel (send + receive);
    // reassembly_budget is the aggregate backstop that gates admit independently, so it
    // must exceed the 16 MB target for that message to be reassembled. Both ends must
    // raise the ceiling — the receive ceiling defends the SUBSCRIBER's own memory.
    constexpr std::size_t k_message_16mb = 16u * 1024u * 1024u;
    // The node ceiling is the 16 MB message plus a small framing allowance (the pub/sub
    // frame carries the topic + a header above the raw payload, so the on-wire payload_len
    // is a touch above the message itself).
    constexpr std::size_t k_node_ceiling = k_message_16mb + 1u * 1024u * 1024u;   // raised above the 8 MiB default
    constexpr std::size_t k_reassembly_budget = 48u * 1024u * 1024u;              // room for the 16 MB message

    // The receive-side ceiling rides the inbound config; the send-side queue must also hold
    // the whole framed message under congestion=block, or a publish would stall instead of
    // completing — so the write-queue byte budget is raised to match.
    wire::stream_inbound_config cfg{};
    constexpr std::size_t k_write_queue = k_node_ceiling + 2u * 1024u * 1024u;

    auto make_transport = [&] {
        return pasio::asio_transport{
            io, cfg, /*no_delay=*/true, pio::congestion::block, k_write_queue,
            /*socket_options=*/{}, k_node_ceiling, k_reassembly_budget};
    };
    auto pub_transport = make_transport();
    auto sub_transport = make_transport();

    // One shared table: each node advertises its own card and browses the other's, so a
    // single process converges the link with no external discovery.
    plexus::discovery::static_discovery disc{{}};

    node_t pub_node{io, disc, "size-envelope-publisher", pub_transport,
                    node_opts("size-envelope-publisher", /*eager=*/false, 0x5152E1)};
    node_t sub_node{io, disc, "size-envelope-subscriber", sub_transport,
                    node_opts("size-envelope-subscriber", /*eager=*/true, 0x5152E2)};
    pub_node.listen({"tcp", "127.0.0.1:55810"});
    sub_node.listen({"tcp", "127.0.0.1:55811"});

    // The per-topic override field + the pure resolver: a topic that sets max_message_bytes
    // raises (or tightens) its OWN ceiling above (or below) the node default; an unset topic
    // falls back to the node default. io::effective_max is the resolution rule.
    plexus::topic_qos frames_qos{};
    frames_qos.max_message_bytes = k_message_16mb;   // an explicit per-topic ceiling for this topic

    std::vector<std::byte> frames_received;
    int oversize_received = 0;
    plexus::subscriber<> frames_sub{
        sub_node, "frames",
        [&](std::span<const std::byte> bytes, const pio::message_info &) {
            frames_received.assign(bytes.begin(), bytes.end());
        }};
    plexus::subscriber<> oversize_sub{
        sub_node, "oversize",
        [&](std::span<const std::byte>, const pio::message_info &) { ++oversize_received; }};

    plexus::publisher<> frames_pub{pub_node, "frames", frames_qos};
    plexus::publisher<> oversize_pub{pub_node, "oversize"};

    // Let the eager dialer establish the session and both demands settle before publishing.
    pump_until(io, [] { return false; }, std::chrono::seconds{1});

    std::cout << "shipped node default : " << (pio::global_default_max_message_bytes >> 20) << " MiB\n";
    std::cout << "configured ceiling   : " << (k_node_ceiling >> 20) << " MiB\n";
    std::cout << "reassembly budget    : " << (k_reassembly_budget >> 20) << " MiB\n";
    std::cout << "\"frames\" effective max: "
              << (pio::effective_max(frames_qos, k_node_ceiling) >> 20)
              << " MiB (per-topic override resolved by io::effective_max)\n";

    // The 16 MB single message: at the raised ceiling and budget it round-trips byte-equal.
    const auto frame = make_payload(k_message_16mb);
    frames_pub.publish(std::span<const std::byte>{frame});
    pump_until(io, [&] { return frames_received.size() == k_message_16mb; });

    const bool frame_ok = equal_bytes(frames_received, frame);
    std::cout << "\n16 MB \"frames\" round-trip: received " << frames_received.size()
              << " bytes, byte-identical=" << std::boolalpha << frame_ok << '\n';

    // A message past the ceiling: 18 MiB exceeds the 16 MiB receive ceiling, so the
    // subscriber's reassembler refuses it (payload_too_large) and it is never delivered.
    const auto oversize = make_payload(18u * 1024u * 1024u);
    oversize_pub.publish(std::span<const std::byte>{oversize});
    pump_until(io, [] { return false; }, std::chrono::seconds{1});   // grace window for a (non-)delivery

    std::cout << "18 MiB oversize publish : deliveries=" << oversize_received
              << " (0 == refused above the ceiling)\n";

    return (frame_ok && oversize_received == 0) ? 0 : 1;
}
