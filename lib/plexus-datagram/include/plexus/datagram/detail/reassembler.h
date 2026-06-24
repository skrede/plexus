#ifndef HPP_GUARD_PLEXUS_DATAGRAM_DETAIL_REASSEMBLER_H
#define HPP_GUARD_PLEXUS_DATAGRAM_DETAIL_REASSEMBLER_H

#include "plexus/policy.h"

#include "plexus/datagram/detail/reassembler_flat_map.h"

#include "plexus/detail/compat.h"

#include "plexus/io/fragmentation.h"
#include "plexus/io/detail/drop_event.h"

#include "plexus/wire/frame_reassembler.h"

#include <span>
#include <memory>
#include <chrono>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <system_error>

namespace plexus::datagram::detail {

// Holds out-of-order fragments keyed by msg_id until a message completes, then hands the
// assembled bytes to an owner-installed sink once. feed() is the untrusted-input surface: it
// range-checks every field before indexing, has no exception path, and returns a drop outcome on
// any malformed/over-budget shape. The total-memory cap is the hard DoS bound and counts both held
// payload and the per-entry slot/present metadata a claimed frag_cnt forces (charged at
// open_entry), so a tiny datagram claiming a huge frag_cnt cannot mint an outsized slot table.
template<typename Executor, typename Timer>
    requires plexus::timer<Timer> && std::constructible_from<Timer, Executor>
class reassembler
{
public:
    using deliver_sink = plexus::detail::move_only_function<void(wire::shared_bytes)>;

    struct config
    {
        std::size_t max_message_size{plexus::io::global_default_max_message_bytes};
        std::size_t total_memory_cap{plexus::io::reassembly_memory_budget};
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

    using drop_sink = plexus::detail::move_only_function<void(const plexus::io::detail::drop_event &)>;

    void on_deliver(deliver_sink cb)
    {
        m_on_deliver = std::move(cb);
    }

    void on_drop(drop_sink cb)
    {
        m_on_drop = std::move(cb);
    }

    // Range-checks precede every index; once all frag_cnt fragments for one msg_id have arrived
    // (in any order) the message assembles and fires on_deliver exactly once.
    outcome feed(std::uint16_t msg_id, std::uint32_t frag_idx, std::uint32_t frag_cnt, std::span<const std::byte> bytes)
    {
        const outcome o =
                (frag_cnt == 0 || frag_idx >= frag_cnt || frag_cnt > plexus::io::max_fragment_count) ? outcome::dropped_malformed : admit_fragment(msg_id, frag_idx, frag_cnt, bytes);
        if(o == outcome::dropped_malformed)
            emit_drop(plexus::io::detail::drop_cause::malformed);
        else if(o == outcome::dropped_cap)
            emit_drop(plexus::io::detail::drop_cause::reassembly_cap);
        return o;
    }

    void cancel()
    {
        m_dead = true;
        for(auto &kv : m_table)
            kv.second.timer->cancel();
    }

    std::size_t in_flight() const noexcept
    {
        return m_table.size();
    }
    std::size_t held_bytes() const noexcept
    {
        return m_held;
    }

    // The per-entry slot/present cost a claimed frag_cnt forces, charged against the same cap as
    // payload so the slot table cannot be minted past the memory bound.
    static constexpr std::size_t structural_cost(std::uint32_t frag_cnt) noexcept
    {
        return static_cast<std::size_t>(frag_cnt) * sizeof(std::vector<std::byte>) + (static_cast<std::size_t>(frag_cnt) + 7u) / 8u;
    }

private:
    struct entry
    {
        std::vector<std::vector<std::byte>> slots;
        std::vector<bool> present;
        std::size_t received{0};
        std::size_t size{0};     // payload bytes held by this entry
        std::size_t overhead{0}; // structural cost charged to the cap
        std::unique_ptr<Timer> timer;
    };

    outcome admit_fragment(std::uint16_t msg_id, std::uint32_t frag_idx, std::uint32_t frag_cnt, std::span<const std::byte> bytes)
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

    typename reassembler_flat_map<entry>::iterator open_entry(std::uint16_t msg_id, std::uint32_t frag_cnt, std::size_t payload)
    {
        if(m_held + structural_cost(frag_cnt) + payload > m_cfg.total_memory_cap)
            return m_table.end(); // structural + payload cost cannot fit the cap
        entry e;
        e.slots.resize(frag_cnt);
        e.present.assign(frag_cnt, false);
        e.overhead = structural_cost(frag_cnt);
        e.timer    = std::make_unique<Timer>(m_executor);
        auto it    = m_table.emplace(msg_id, std::move(e)); // refuses past the cap-implied count ceiling
        if(it == m_table.end())
            return it;
        m_held += it->second.overhead; // charge the structural cost up front
        arm_timeout(msg_id, it->second);
        return it;
    }

    outcome store(entry &e, std::uint16_t msg_id, std::uint32_t frag_idx, std::span<const std::byte> bytes)
    {
        if(frag_idx >= e.slots.size())
            return outcome::dropped_malformed; // frag_cnt disagreed with the open entry
        if(e.present[frag_idx])
            return outcome::admitted; // duplicate / overlap — ignore, keep the first
        if(e.size + bytes.size() > m_cfg.max_message_size || m_held + bytes.size() > m_cfg.total_memory_cap)
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
            m_on_deliver(wire::shared_bytes{std::move(msg)});
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
                    emit_drop(plexus::io::detail::drop_cause::reassembly_evicted);
                });
    }

    void emit_drop(plexus::io::detail::drop_cause cause)
    {
        if(m_on_drop)
            m_on_drop(plexus::io::detail::drop_event{.cause = cause, .transport = plexus::io::locality::remote});
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

    Executor m_executor;
    config m_cfg;
    reassembler_flat_map<entry> m_table;
    std::size_t m_held{0};
    bool m_dead{false};
    deliver_sink m_on_deliver;
    drop_sink m_on_drop;
};

}

#endif
