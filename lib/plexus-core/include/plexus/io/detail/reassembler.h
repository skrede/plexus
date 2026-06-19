#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_REASSEMBLER_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_REASSEMBLER_H

#include "plexus/io/fragmentation.h"
#include "plexus/io/detail/drop_event.h"
#include "plexus/io/detail/reassembler_flat_map.h"

#include "plexus/wire/frame_reassembler.h"

#include "plexus/policy.h"
#include "plexus/detail/compat.h"

#include <span>
#include <memory>
#include <chrono>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <system_error>

namespace plexus::io::detail {

// over-limit: one reassembly window; the fragment-insert, hole-track, and complete-and-deliver
// steps all operate over the one partial-message buffer, so splitting them scatters the
// offset/hole state.

// The bounded receiver half of the fragment/reassemble block: a partial-message table keyed by
// msg_id (a bounded sorted-vector flat map, alloc-free in steady state) holding out-of-order
// fragments until a message completes, then handing the assembled bytes to an owner-installed sink
// ONCE. feed() is the untrusted-input surface — it range-checks every field BEFORE indexing, has no
// exception path, and returns a drop outcome on any malformed/over-budget shape.
//
// BOUNDS (required-WITH-default config, structural — not setters; sweep-substantiated):
// a per-message reassembly ceiling (max_message_size); a TOTAL reassembly-memory cap across all
// in-flight entries (the hard DoS bound — a new partial that would exceed it is rejected, so many
// small fragments claiming a huge total cannot exhaust memory). The cap counts BOTH held payload
// AND the per-entry slot/present metadata a claimed frag_cnt forces (charged at open_entry), so a
// tiny datagram claiming frag_cnt=32768 cannot mint ~786 KB of slot table accounted as a single
// payload byte — the metadata is bounded by the same cap, not just the payload. A per-message
// timeout (the Policy timer reclaims a stalled best-effort partial whose fragments never complete)
// closes the slow path. The cap default (reassembly_memory_budget = 16 MiB) admits two concurrent
// full global_default_max_message_bytes (8 MiB) partials per peer while bounding the aggregate
// worst case (the demux peer cap × this) — the always-on aggregate DoS bound regardless of the
// per-topic ceiling. The timeout default (5000 ms) gives an honest slow message room to complete
// while reclaiming a stalled partial within five seconds.
//
// LIFETIME: single-owner, bare `this`, no shared lifetime. Each in-flight entry owns its timeout
// timer; the owning channel cancels the reassembler before it dies, so a timer firing after
// teardown is a guarded no-op (the if(ec || m_dead) guard). Sans-IO: on_deliver fires synchronously
// on completion (the channel posts on_data around it; the block never touches the io_context).
template<typename Executor, typename Timer>
    requires plexus::timer<Timer> && std::constructible_from<Timer, Executor>
class reassembler
{
public:
    // Takes the assembled message as an owning shared_bytes: it materializes ONCE and the owner
    // rides straight to delivery, posted without a re-copy (the stream side's owner-carry idiom).
    using deliver_sink = plexus::detail::move_only_function<void(wire::shared_bytes)>;

    struct config
    {
        std::size_t               max_message_size{global_default_max_message_bytes};
        std::size_t               total_memory_cap{reassembly_memory_budget};
        std::chrono::milliseconds per_message_timeout{5000};
    };

    enum class outcome : std::uint8_t
    {
        admitted,          // fragment stored; the message is still incomplete
        completed,         // this fragment finished the message (on_deliver fired)
        dropped_malformed, // idx>=cnt, oversize cnt, or a per-message-ceiling overrun
        dropped_cap,       // admitting a new partial would breach the total-memory cap
    };

    explicit reassembler(Executor executor, config cfg = {})
            : m_executor(executor)
            , m_cfg(cfg)
            , m_table(cap_implied_max_partials(cfg.total_memory_cap, structural_cost(1)))
    {
    }

    reassembler(const reassembler &)            = delete;
    reassembler &operator=(const reassembler &) = delete;

    using drop_sink = plexus::detail::move_only_function<void(const drop_event &)>;

    void on_deliver(deliver_sink cb) { m_on_deliver = std::move(cb); }

    // The drop-observability seam (null by default — zero cost when unobserved). A malformed/
    // over-cap fragment and a timed-out partial each hand a coalesced event here; the installed
    // sink POSTS, so the receive site never fires the observer synchronously.
    void on_drop(drop_sink cb) { m_on_drop = std::move(cb); }

    // Feed an untrusted fragment. Range-checks precede every index; once all frag_cnt fragments
    // for one msg_id have arrived (in any order) the message assembles and fires on_deliver
    // exactly once. Returns the per-fragment outcome.
    outcome feed(std::uint16_t msg_id, std::uint32_t frag_idx, std::uint32_t frag_cnt,
                 std::span<const std::byte> bytes)
    {
        const outcome o = (frag_cnt == 0 || frag_idx >= frag_cnt || frag_cnt > max_fragment_count)
                ? outcome::dropped_malformed
                : admit_fragment(msg_id, frag_idx, frag_cnt, bytes);
        if(o == outcome::dropped_malformed)
            emit_drop(drop_cause::malformed);
        else if(o == outcome::dropped_cap)
            emit_drop(drop_cause::reassembly_cap);
        return o;
    }

    // Owner teardown: stop every timer firing into a dying reassembler.
    void cancel()
    {
        m_dead = true;
        for(auto &kv : m_table)
            kv.second.timer->cancel();
    }

    [[nodiscard]] std::size_t in_flight() const noexcept { return m_table.size(); }
    [[nodiscard]] std::size_t held_bytes() const noexcept { return m_held; }

    // The per-entry structural cost a claimed frag_cnt forces (the slots vector plus the present
    // bitmap), charged against the same cap as payload — see the BOUNDS note at the class head.
    [[nodiscard]] static constexpr std::size_t structural_cost(std::uint32_t frag_cnt) noexcept
    {
        return static_cast<std::size_t>(frag_cnt) * sizeof(std::vector<std::byte>) +
                (static_cast<std::size_t>(frag_cnt) + 7u) / 8u;
    }

private:
    struct entry
    {
        std::vector<std::vector<std::byte>> slots; // per-index fragment bytes
        std::vector<bool>                   present;
        std::size_t                         received{0};
        std::size_t                         size{0}; // payload bytes held by this entry
        std::size_t            overhead{0}; // structural slot/present cost charged to the cap
        std::unique_ptr<Timer> timer;
    };

    outcome admit_fragment(std::uint16_t msg_id, std::uint32_t frag_idx, std::uint32_t frag_cnt,
                           std::span<const std::byte> bytes)
    {
        auto it = m_table.find(msg_id);
        if(it == m_table.end())
        {
            it = open_entry(msg_id, frag_cnt, bytes.size());
            if(it == m_table.end())
                return outcome::dropped_cap; // the byte cap or the count ceiling refused
        }
        return store(it->second, msg_id, frag_idx, bytes);
    }

    typename reassembler_flat_map<entry>::iterator
    open_entry(std::uint16_t msg_id, std::uint32_t frag_cnt, std::size_t payload)
    {
        if(m_held + structural_cost(frag_cnt) + payload > m_cfg.total_memory_cap)
            return m_table.end(); // structural + payload cost cannot fit the cap
        entry e;
        e.slots.resize(frag_cnt);
        e.present.assign(frag_cnt, false);
        e.overhead = structural_cost(frag_cnt);
        e.timer    = std::make_unique<Timer>(m_executor);
        auto it =
                m_table.emplace(msg_id, std::move(e)); // refuses past the cap-implied count ceiling
        if(it == m_table.end())
            return it;
        m_held += it->second.overhead; // charge the structural cost up front
        arm_timeout(msg_id, it->second);
        return it;
    }

    outcome store(entry &e, std::uint16_t msg_id, std::uint32_t frag_idx,
                  std::span<const std::byte> bytes)
    {
        if(frag_idx >= e.slots.size())
            return outcome::dropped_malformed; // frag_cnt disagreed with the open entry
        if(e.present[frag_idx])
            return outcome::admitted; // duplicate / overlap — ignore, keep the first
        if(e.size + bytes.size() > m_cfg.max_message_size ||
           m_held + bytes.size() > m_cfg.total_memory_cap)
            return outcome::dropped_malformed; // per-message ceiling / total cap overrun

        e.slots[frag_idx].assign(bytes.begin(), bytes.end());
        e.present[frag_idx] = true;
        e.size += bytes.size();
        m_held += bytes.size();
        ++e.received;

        if(e.received < e.slots.size())
            return outcome::admitted;
        assemble_and_evict(msg_id, e);
        return outcome::completed;
    }

    void assemble_and_evict(std::uint16_t msg_id, entry &e)
    {
        std::vector<std::byte> msg;
        msg.reserve(e.size);
        for(auto &slot : e.slots)
            msg.insert(msg.end(), slot.begin(), slot.end());
        if(m_on_deliver)
            m_on_deliver(wire::shared_bytes{std::move(msg)}); // one owned materialization rides up
        evict(msg_id);
    }

    void arm_timeout(std::uint16_t msg_id, entry &e)
    {
        e.timer->expires_after(m_cfg.per_message_timeout);
        e.timer->async_wait(
                [this, msg_id](std::error_code ec)
                {
                    if(ec || m_dead)
                        return;    // cancelled by completion / teardown
                    evict(msg_id); // best-effort reclaim of the stalled partial
                    emit_drop(drop_cause::reassembly_evicted);
                });
    }

    void emit_drop(drop_cause cause)
    {
        if(m_on_drop)
            m_on_drop(drop_event{.cause = cause, .transport = locality::remote});
    }

    void evict(std::uint16_t msg_id)
    {
        auto it = m_table.find(msg_id);
        if(it == m_table.end())
            return;
        it->second.timer->cancel();
        m_held -= it->second.size + it->second.overhead;
        m_table.erase(it);
    }

    Executor                    m_executor;
    config                      m_cfg;
    reassembler_flat_map<entry> m_table;
    std::size_t                 m_held{0};
    bool                        m_dead{false};
    deliver_sink                m_on_deliver;
    drop_sink                   m_on_drop;
};

}

#endif
