#ifndef HPP_GUARD_PLEXUS_DETAIL_LOAN_POOL_H
#define HPP_GUARD_PLEXUS_DETAIL_LOAN_POOL_H

#include "plexus/io/object_carrier.h"

#include <new>
#include <cstdint>
#include <cstddef>
#include <utility>

namespace plexus::detail {

template<typename T>
class loan_pool;

// A move-only handle to a pool slot holding a live T. The handle owns the slot until it
// is either published (the publisher nulls the handle's slot pointer, transferring the
// release obligation to the in-flight carrier refcount) or destroyed (an un-published
// handle returns its slot to the pool). Dereferences to the constructed T&.
//
// A published handle is left null, so its destructor releases nothing — the
// double-release a published-then-destroyed handle would otherwise cause is closed by
// nulling, asserted in debug.
template<typename T>
class loan
{
public:
    loan() = default;

    loan(loan &&other) noexcept
            : m_slot(std::exchange(other.m_slot, nullptr))
    {
    }

    loan &operator=(loan &&other) noexcept
    {
        if(this != &other)
        {
            release_if_held();
            m_slot = std::exchange(other.m_slot, nullptr);
        }
        return *this;
    }

    loan(const loan &)            = delete;
    loan &operator=(const loan &) = delete;

    ~loan() { release_if_held(); }

    [[nodiscard]] bool valid() const noexcept { return m_slot != nullptr; }
    explicit           operator bool() const noexcept { return valid(); }

    T &operator*() const noexcept { return *object_ptr(); }
    T *operator->() const noexcept { return object_ptr(); }

private:
    friend class loan_pool<T>;

    explicit loan(io::loan_slot *slot) noexcept
            : m_slot(slot)
    {
    }

    T *object_ptr() const noexcept
    {
        return const_cast<T *>(static_cast<const T *>(m_slot->object));
    }

    // The publish hand-off: surrender the slot to the carrier (whose refcount now owns
    // the release) and leave the handle null so its destructor is a no-op.
    io::loan_slot *take() noexcept { return std::exchange(m_slot, nullptr); }

    void release_if_held() noexcept
    {
        if(m_slot != nullptr)
        {
            io::object_carrier carrier{};
            carrier.slot = std::exchange(m_slot, nullptr);
            io::release(carrier);
        }
    }

    io::loan_slot *m_slot{};
};

// A fixed-capacity, alloc-free object pool for T. The slot array (the ONLY allocation)
// is built once at construction; thereafter borrow/publish/release reuse slots through an
// intrusive freelist with no further allocation. Each slot carries an io::loan_slot
// control so a published object rides the zero-serialization lane by reference; the per-T
// static release destroys the object in place and returns the slot to the freelist.
//
// Exhaustion (no free slot) is NOT an error and NEVER blocks or grows: try_borrow returns
// an empty loan and the publisher degrades to the serialize path with a counter.
template<typename T>
class loan_pool
{
public:
    explicit loan_pool(std::size_t capacity)
            : m_slots(new node[capacity])
            , m_capacity(capacity)
    {
        for(std::size_t i = 0; i < capacity; ++i)
        {
            m_slots[i].owner           = this;
            m_slots[i].control.refs    = 0;
            m_slots[i].control.release = &release_slot;
            m_slots[i].next_free       = m_free;
            m_free                     = &m_slots[i];
        }
    }

    ~loan_pool() { delete[] m_slots; }

    loan_pool(const loan_pool &)            = delete;
    loan_pool &operator=(const loan_pool &) = delete;

    loan_pool(loan_pool &&other) noexcept
            : m_slots(std::exchange(other.m_slots, nullptr))
            , m_free(std::exchange(other.m_free, nullptr))
            , m_capacity(std::exchange(other.m_capacity, 0))
    {
        restamp_owner();
    }

    loan_pool &operator=(loan_pool &&other) noexcept
    {
        if(this != &other)
        {
            delete[] m_slots;
            m_slots    = std::exchange(other.m_slots, nullptr);
            m_free     = std::exchange(other.m_free, nullptr);
            m_capacity = std::exchange(other.m_capacity, 0);
            restamp_owner();
        }
        return *this;
    }

    // Borrow a slot and construct a T in place from the forwarded arguments. Returns an
    // empty loan on exhaustion. The slot's refcount starts at 1 (the loan's own
    // reference); a publish transfers that reference to the in-flight carrier.
    template<typename... Args>
    loan<T> try_borrow(Args &&...args)
    {
        node *n = pop_free();
        if(n == nullptr)
            return loan<T>{};
        T *obj            = ::new(static_cast<void *>(&n->storage)) T(std::forward<Args>(args)...);
        n->control.object = obj;
        n->control.refs   = 1;
        return loan<T>{&n->control};
    }

    // Build the carrier for a borrowed loan and surrender the slot to it: the loan is
    // left null (its release obligation passed to the carrier's refcount), the carrier
    // carries the type witness and the live slot. The forwarder addrefs per fast-path
    // send and releases once after the fan loop, so the slot is balanced on every path.
    static io::object_carrier carrier_for(loan<T> &held, std::uint64_t type_tag) noexcept
    {
        io::object_carrier carrier{};
        carrier.type_tag   = type_tag;
        carrier.native_key = &io::detail::type_key<T>;
        carrier.slot       = held.take();
        return carrier;
    }

    [[nodiscard]] std::size_t capacity() const noexcept { return m_capacity; }

private:
    struct node
    {
        alignas(T) std::byte storage[sizeof(T)];
        io::loan_slot control;
        loan_pool    *owner;
        node         *next_free;
    };

    // Destroy the object in place and return its slot to the freelist. The node is
    // recovered from the control member's offset, and its pool from the back-pointer
    // stamped at build — the producer's concrete type is known statically here (the per-T
    // release), never a cast across an erased boundary.
    static void release_slot(io::loan_slot *control) noexcept
    {
        node *n = reinterpret_cast<node *>(reinterpret_cast<std::byte *>(control) -
                                           offsetof(node, control));
        std::launder(reinterpret_cast<T *>(&n->storage))->~T();
        control->object = nullptr;
        n->owner->push_free(n);
    }

    // The move steals the node array by pointer, so every m_slot (&n->control) and every
    // next_free link stays valid — but each slot's owner still names the moved-from pool.
    // Re-stamp it: release_slot routes a freed slot through n->owner->push_free, so a stale
    // owner would push onto a dead pool's freelist (a use-after-free).
    void restamp_owner() noexcept
    {
        for(std::size_t i = 0; i < m_capacity; ++i)
            m_slots[i].owner = this;
    }

    node *pop_free() noexcept
    {
        if(m_free == nullptr)
            return nullptr;
        node *n = m_free;
        m_free  = n->next_free;
        return n;
    }

    void push_free(node *n) noexcept
    {
        n->next_free = m_free;
        m_free       = n;
    }

    node       *m_slots;
    node       *m_free{};
    std::size_t m_capacity;
};

}

#endif
