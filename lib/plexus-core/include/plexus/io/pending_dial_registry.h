#ifndef HPP_GUARD_PLEXUS_IO_PENDING_DIAL_REGISTRY_H
#define HPP_GUARD_PLEXUS_IO_PENDING_DIAL_REGISTRY_H

#include "plexus/detail/compat.h"

#include <memory>
#include <variant>
#include <utility>
#include <unordered_map>

namespace plexus::io {

// The half-open dial table + the accepted table + their ownership-transfer, hoisted
// out of every datagram transport so a plain UDP backend and a crypto backend reuse
// the SAME block instead of re-inlining three near-identical maps. The transport owns
// each minted channel (unique_ptr) keyed by its raw pointer; the registry holds the
// two tables and the strictly-shaped transfer verbs. Generic over Channel and a
// per-entry Payload (the ARQ for UDP, an empty std::monostate for a crypto backend
// that owns its retransmit in-channel), so the block forks for neither.
//
// Two lifetime hazards are designed-out at the API shape, not left to the caller:
//
//   COPY-BEFORE-ERASE: resolve() moves the channel out into a LOCAL and erases the
//   entry BEFORE returning — it never reads the entry after the erase. The caller
//   copies any endpoint/value it carries OUT before calling, because the value is
//   typically bound to the erased entry's own closure capture; re-reading it after
//   the erase is a use-after-free.
//
//   DEFERRED-DESTROY: a fail edge may fire from INSIDE the channel's own member call
//   (its deliver_inbound / drain stack), so destroying the channel synchronously
//   there frees it mid-stack — a use-after-free as that stack unwinds. fail() never
//   destroys the channel itself: it moves the channel out and routes it through an
//   injected defer-destroy callback that the owner posts to a continuation, so the
//   channel is destroyed only after the current stack unwinds. The UDP path (whose
//   fail is a clean timer callback) adopts the same defer harmlessly — strictly safe.
//
// The flood cap stays in the transport demux (insert is called only on an admitted
// entry), so the registry holds only admitted entries and never grows unbounded.
// Single-owner, no shared lifetime — the owner clears the block before it dies.
template<typename Channel, typename Payload = std::monostate>
class pending_dial_registry
{
public:
    // The defer-destroy sink: hand it the freed channel and it destroys it OFF the
    // current stack (a posted continuation that owns it until it runs). Installed at
    // construction; fail() routes the freed channel through it.
    using defer_destroy = plexus::detail::move_only_function<void(std::unique_ptr<Channel>)>;

    explicit pending_dial_registry(defer_destroy defer)
            : m_defer(std::move(defer))
    {
    }

    // Book a half-open dial entry keyed by the raw channel pointer.
    void insert(Channel *raw, std::unique_ptr<Channel> channel, Payload payload = {})
    {
        m_pending.insert_or_assign(raw, entry{std::move(channel), std::move(payload)});
    }

    // Move the channel out into a LOCAL, erase the entry, THEN return — the entry is
    // never read after the erase (copy-before-erase: the caller copies any value the
    // entry carries OUT before calling). Returns nullptr on a miss.
    [[nodiscard]] std::unique_ptr<Channel> resolve(Channel *raw)
    {
        auto it = m_pending.find(raw);
        if(it == m_pending.end())
            return nullptr;
        auto channel = std::move(it->second.channel);
        m_pending.erase(it);
        return channel;
    }

    // Drop a half-open dial entry, destroying its channel DEFERRED (never synchronously
    // from inside the channel's own member call). Copy-before-erase applies to the
    // caller exactly as for resolve(). A no-op on a miss.
    void fail(Channel *raw)
    {
        auto it = m_pending.find(raw);
        if(it == m_pending.end())
            return;
        auto channel = std::move(it->second.channel);
        m_pending.erase(it);
        if(m_defer)
            m_defer(std::move(channel));
    }

    // Read the per-entry payload (the ARQ for UDP); nullptr on a miss.
    [[nodiscard]] Payload *payload_of(Channel *raw)
    {
        auto it = m_pending.find(raw);
        return it == m_pending.end() ? nullptr : &it->second.payload;
    }

    // The accepted table: a server-minted channel awaiting its hand-off.
    void insert_accepted(Channel *raw, std::unique_ptr<Channel> channel)
    {
        m_accepted.insert_or_assign(raw, std::move(channel));
    }

    // Move an accepted channel out, erase, return (the demux raw ref stays intact — the
    // owner keeps routing inbound by endpoint). Returns nullptr on a miss.
    [[nodiscard]] std::unique_ptr<Channel> adopt_accepted(Channel *raw)
    {
        auto it = m_accepted.find(raw);
        if(it == m_accepted.end())
            return nullptr;
        auto channel = std::move(it->second);
        m_accepted.erase(it);
        return channel;
    }

    // Drop an accepted entry, destroying its channel DEFERRED through the SAME injected
    // sink fail() uses — the accept-side analog of fail(). An accept-side handshake fail
    // may fire from INSIDE the channel's own deliver_inbound/drain stack (the triggering
    // datagram is fed straight into the freshly-minted accept channel), so destroying it
    // synchronously there frees it mid-stack — a use-after-free as that stack unwinds.
    // Moves the channel out, erases the entry, then routes it through m_defer. A no-op on
    // a miss. Copy-before-erase applies to the caller exactly as for resolve()/fail().
    void fail_accepted(Channel *raw)
    {
        auto it = m_accepted.find(raw);
        if(it == m_accepted.end())
            return;
        auto channel = std::move(it->second);
        m_accepted.erase(it);
        if(m_defer)
            m_defer(std::move(channel));
    }

    [[nodiscard]] std::size_t pending_size() const noexcept
    {
        return m_pending.size();
    }

    [[nodiscard]] std::size_t accepted_size() const noexcept
    {
        return m_accepted.size();
    }

    // Drop both tables (teardown). Destroys the held channels synchronously — the owner
    // calls clear() from its own close path, not from inside a channel's member call.
    void clear()
    {
        m_pending.clear();
        m_accepted.clear();
    }

private:
    struct entry
    {
        std::unique_ptr<Channel> channel;
        Payload                  payload;
    };

    defer_destroy                                           m_defer;
    std::unordered_map<Channel *, entry>                    m_pending;
    std::unordered_map<Channel *, std::unique_ptr<Channel>> m_accepted;
};

}

#endif
