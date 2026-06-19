#ifndef HPP_GUARD_PLEXUS_TESTS_SUPPORT_LOSS_REORDER_SHIM_H
#define HPP_GUARD_PLEXUS_TESTS_SUPPORT_LOSS_REORDER_SHIM_H

#include <asio/buffer.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/udp.hpp>

#include <deque>
#include <array>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <algorithm>

// A deterministic, fixed-seed loss/reorder relay sitting on the datagram wire between a
// client and a server UDP socket. Every datagram the client emits to the relay's front
// port is forwarded to the server, optionally DROPPED (a configurable fraction) or held
// and re-emitted out of order (a bounded reorder depth). The drop/reorder decisions come
// from an LCG seeded at construction, so the captured-then-re-injected datagram sequence
// is byte-identical across process runs — the empirical-reproducibility property the
// fragment-scale sweep and the lossy-link benchmark cell both depend on (no std::random).
//
// It lives test-side only (never plexus source): it interposes at the test's wire
// boundary, not inside any plexus channel. Header-only so a second translation unit reuses
// the same shim — every member is inline.

namespace plexus::testing {

// A deterministic LCG (Numerical Recipes constants), copied verbatim from the anti-replay
// window sweep so the loss schedule is byte-identical across runs and platforms.
class shim_lcg
{
public:
    explicit shim_lcg(std::uint64_t seed) noexcept
            : m_state(seed)
    {
    }

    std::uint64_t next() noexcept
    {
        m_state = m_state * 6364136223846793005ull + 1442695040888963407ull;
        return m_state >> 17u;
    }

    // A fixed-point fraction comparison in [0, 1): returns true with probability ~num/den.
    bool hits(std::uint32_t num, std::uint32_t den) noexcept
    {
        return den != 0 && static_cast<std::uint32_t>(next() % den) < num;
    }

    std::size_t bounded(std::size_t n) noexcept
    {
        return n == 0 ? 0 : static_cast<std::size_t>(next() % n);
    }

private:
    std::uint64_t m_state;
};

// The shim's configuration: a loss fraction expressed as loss_num/loss_den (so a
// fixed-seed integer decision stays RNG-free), a bounded reorder depth (0 = in order),
// and the LCG seed. Required-with-default knobs set once at construction — never a mutable
// setter (the determinism contract: the schedule is fixed at construction).
struct loss_reorder_config
{
    std::uint32_t loss_num      = 0;
    std::uint32_t loss_den      = 100;
    std::size_t   reorder_depth = 0;
    std::uint64_t seed          = 0x9E3779B97F4A7C15ull;
};

// A pure (no-IO) deterministic loss/reorder scheduler over an opaque datagram stream.
// drive() consumes one datagram and returns the SET of datagrams to emit now (possibly
// empty if it is dropped or held for reorder; possibly several when a held burst flushes).
// flush() drains any datagrams still held in the reorder buffer at end-of-stream. The
// emitted order across a fixed input + seed is identical on every run, asserted by the
// shim's own self-test.
class loss_reorder_scheduler
{
public:
    explicit loss_reorder_scheduler(loss_reorder_config cfg)
            : m_cfg(cfg)
            , m_rng(cfg.seed)
    {
    }

    [[nodiscard]] std::vector<std::vector<std::byte>> drive(std::span<const std::byte> datagram)
    {
        std::vector<std::vector<std::byte>> out;
        if(m_rng.hits(m_cfg.loss_num, m_cfg.loss_den))
        {
            ++m_dropped;
            return out;
        }
        std::vector<std::byte> dg(datagram.begin(), datagram.end());
        if(m_cfg.reorder_depth == 0)
        {
            out.push_back(std::move(dg));
            return out;
        }
        m_hold.push_back(std::move(dg));
        if(m_hold.size() > m_cfg.reorder_depth)
            out.push_back(take_held());
        return out;
    }

    [[nodiscard]] std::vector<std::vector<std::byte>> flush()
    {
        std::vector<std::vector<std::byte>> out;
        while(!m_hold.empty())
            out.push_back(take_held());
        return out;
    }

    [[nodiscard]] std::size_t dropped() const noexcept { return m_dropped; }

private:
    // Pull one held datagram at a seed-chosen position so the re-emission order is shuffled
    // within the bounded window (not strictly FIFO) yet deterministic.
    std::vector<std::byte> take_held()
    {
        const std::size_t      idx = m_rng.bounded(m_hold.size());
        std::vector<std::byte> dg  = std::move(m_hold[idx]);
        m_hold.erase(m_hold.begin() + static_cast<std::ptrdiff_t>(idx));
        return dg;
    }

    loss_reorder_config                m_cfg;
    shim_lcg                           m_rng;
    std::deque<std::vector<std::byte>> m_hold;
    std::size_t                        m_dropped = 0;
};

// A live UDP relay applying the deterministic scheduler at the wire boundary: the client
// dials the relay's front port; the relay forwards (per the scheduler) to the real server
// and pipes the server's replies straight back, untouched (the loss model is one-directional
// on the data-carrying client->server leg, matching the injected-loss test legs).
class loss_reorder_relay
{
public:
    loss_reorder_relay(::asio::io_context &io, std::uint16_t server_port, loss_reorder_config cfg)
            : m_io(io)
            , m_front(io, ::asio::ip::udp::endpoint(::asio::ip::udp::v4(), 0))
            , m_back(io, ::asio::ip::udp::endpoint(::asio::ip::udp::v4(), 0))
            , m_server_ep(::asio::ip::make_address("127.0.0.1"), server_port)
            , m_sched(cfg)
    {
        recv_front();
        recv_back();
    }

    [[nodiscard]] std::uint16_t port() const { return m_front.local_endpoint().port(); }
    [[nodiscard]] std::size_t   dropped() const noexcept { return m_sched.dropped(); }
    [[nodiscard]] std::size_t   forwarded() const noexcept { return m_forwarded; }

private:
    void emit_to_server(std::vector<std::byte> dg)
    {
        ++m_forwarded;
        m_back.send_to(::asio::buffer(dg.data(), dg.size()), m_server_ep);
    }

    void recv_front()
    {
        m_front.async_receive_from(
                ::asio::buffer(m_front_buf), m_from,
                [this](std::error_code ec, std::size_t n)
                {
                    if(ec)
                        return;
                    m_client_ep = m_from;
                    for(auto &dg : m_sched.drive(std::span<const std::byte>{m_front_buf.data(), n}))
                        emit_to_server(std::move(dg));
                    recv_front();
                });
    }

    void recv_back()
    {
        m_back.async_receive_from(::asio::buffer(m_back_buf), m_from,
                                  [this](std::error_code ec, std::size_t n)
                                  {
                                      if(ec)
                                          return;
                                      if(m_client_ep.port() != 0)
                                          m_front.send_to(::asio::buffer(m_back_buf.data(), n),
                                                          m_client_ep);
                                      recv_back();
                                  });
    }

    ::asio::io_context          &m_io;
    ::asio::ip::udp::socket      m_front;
    ::asio::ip::udp::socket      m_back;
    ::asio::ip::udp::endpoint    m_server_ep;
    ::asio::ip::udp::endpoint    m_client_ep;
    ::asio::ip::udp::endpoint    m_from;
    std::array<std::byte, 65536> m_front_buf{};
    std::array<std::byte, 65536> m_back_buf{};
    loss_reorder_scheduler       m_sched;
    std::size_t                  m_forwarded = 0;
};

}

#endif
