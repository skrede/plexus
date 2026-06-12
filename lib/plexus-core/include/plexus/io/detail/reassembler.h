#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_REASSEMBLER_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_REASSEMBLER_H

#include "plexus/io/fragmentation.h"
#include "plexus/io/detail/drop_event.h"

#include "plexus/policy.h"
#include "plexus/detail/compat.h"

#include <map>
#include <span>
#include <memory>
#include <chrono>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <system_error>

namespace plexus::io::detail {

// The bounded receiver half of the fragment/reassemble block: a partial-message table
// keyed by msg_id that holds out-of-order fragments until a message is complete, then
// hands the assembled bytes to an owner-installed sink ONCE. feed() is the new
// untrusted-input surface — it range-checks every field BEFORE indexing, is noexcept in
// spirit (no exception path), and returns a drop outcome on any malformed/over-budget shape.
//
// BOUNDS (required-WITH-default config, structural — not setters; sweep-substantiated):
// a per-message reassembly ceiling (max_message_size); a TOTAL reassembly-memory cap
// across all in-flight entries (the hard DoS bound — a new partial that would exceed it
// is rejected, so many small fragments claiming a huge total cannot exhaust memory); and
// a per-message timeout (the Policy timer reclaims a stalled best-effort partial whose
// fragments never complete). The cap default (16 MiB) admits 4 concurrent full
// max_message_size (4 MiB) partials per peer — multi-message headroom above the single-
// message ceiling — while bounding the aggregate worst case (the demux peer cap × this)
// to 64 GiB rather than the 256 GiB a 64 MiB per-peer cap would expose. The timeout
// default (5000 ms) exceeds the ~3355 ms a 4 MiB message takes over a 10 Mbit/s link (so
// an honest slow message on a low-bandwidth control link is not evicted) while reclaiming
// a stalled partial within five seconds.
//
// LIFETIME: single-owner, bare `this`, no shared lifetime (the same discipline as the
// reorder buffer and ARQ). Each in-flight entry owns its timeout timer; the owning channel
// cancels the reassembler before it dies, so a timer firing after teardown is a guarded
// no-op (the if(ec || m_dead) return guard). Sans-IO: on_deliver is called synchronously
// on completion; the channel posts on_data around it (the block never touches the io_context).
template <typename Executor, typename Timer>
    requires plexus::timer<Timer> && std::constructible_from<Timer, Executor>
class reassembler
{
public:
    using deliver_sink = plexus::detail::move_only_function<void(std::span<const std::byte>)>;

    struct config
    {
        std::size_t max_message_size{fragmentation_limits::max_message_size};
        std::size_t total_memory_cap{16u * 1024u * 1024u};
        std::chrono::milliseconds per_message_timeout{5000};
    };

    enum class outcome : std::uint8_t
    {
        admitted,         // fragment stored; the message is still incomplete
        completed,        // this fragment finished the message (on_deliver fired)
        dropped_malformed,// idx>=cnt, oversize cnt, or a per-message-ceiling overrun
        dropped_cap,      // admitting a new partial would breach the total-memory cap
    };

    explicit reassembler(Executor executor, config cfg = {})
        : m_executor(executor)
        , m_cfg(cfg)
    {
    }

    reassembler(const reassembler &) = delete;
    reassembler &operator=(const reassembler &) = delete;

    using drop_sink = plexus::detail::move_only_function<void(const drop_event &)>;

    void on_deliver(deliver_sink cb) { m_on_deliver = std::move(cb); }

    // The drop-observability seam (null by default — zero cost when unobserved). The owner
    // installs the engine's posted drop_sink; a malformed/over-cap fragment and a
    // timed-out partial each hand a coalesced event here. The sink POSTS, so the receive
    // site never fires the observer synchronously.
    void on_drop(drop_sink cb) { m_on_drop = std::move(cb); }

    // Feed an untrusted fragment. Range-checks precede every index; an in-order or
    // out-of-order arrival of all frag_cnt fragments for one msg_id assembles the message
    // and fires on_deliver exactly once. Returns the per-fragment outcome.
    outcome feed(std::uint16_t msg_id, std::uint16_t frag_idx, std::uint16_t frag_cnt,
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

private:
    struct entry
    {
        std::vector<std::vector<std::byte>> slots;   // per-index fragment bytes
        std::vector<bool> present;
        std::size_t received{0};
        std::size_t size{0};                         // bytes held by this entry
        std::unique_ptr<Timer> timer;
    };

    outcome admit_fragment(std::uint16_t msg_id, std::uint16_t frag_idx, std::uint16_t frag_cnt,
                           std::span<const std::byte> bytes)
    {
        auto it = m_table.find(msg_id);
        if(it == m_table.end())
        {
            if(m_held + bytes.size() > m_cfg.total_memory_cap)
                return outcome::dropped_cap;          // a new partial cannot fit the cap
            it = open_entry(msg_id, frag_cnt);
        }
        return store(it->second, msg_id, frag_idx, bytes);
    }

    typename std::map<std::uint16_t, entry>::iterator open_entry(std::uint16_t msg_id, std::uint16_t frag_cnt)
    {
        entry e;
        e.slots.resize(frag_cnt);
        e.present.assign(frag_cnt, false);
        e.timer = std::make_unique<Timer>(m_executor);
        auto [it, _] = m_table.emplace(msg_id, std::move(e));
        arm_timeout(msg_id, it->second);
        return it;
    }

    outcome store(entry &e, std::uint16_t msg_id, std::uint16_t frag_idx, std::span<const std::byte> bytes)
    {
        if(frag_idx >= e.slots.size())
            return outcome::dropped_malformed;        // frag_cnt disagreed with the open entry
        if(e.present[frag_idx])
            return outcome::admitted;                 // duplicate / overlap — ignore, keep the first
        if(e.size + bytes.size() > m_cfg.max_message_size || m_held + bytes.size() > m_cfg.total_memory_cap)
            return outcome::dropped_malformed;        // per-message ceiling / total cap overrun

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
            m_on_deliver(std::span<const std::byte>{msg});
        evict(msg_id);
    }

    void arm_timeout(std::uint16_t msg_id, entry &e)
    {
        e.timer->expires_after(m_cfg.per_message_timeout);
        e.timer->async_wait([this, msg_id](std::error_code ec) {
            if(ec || m_dead)
                return;                               // cancelled by completion / teardown
            evict(msg_id);                            // best-effort reclaim of the stalled partial
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
        m_held -= it->second.size;
        m_table.erase(it);
    }

    Executor m_executor;
    config m_cfg;
    std::map<std::uint16_t, entry> m_table;
    std::size_t m_held{0};
    bool m_dead{false};
    deliver_sink m_on_deliver;
    drop_sink m_on_drop;
};

}

#endif
