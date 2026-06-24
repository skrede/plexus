#ifndef HPP_GUARD_PLEXUS_DETAIL_LOAN_POOL_H
#define HPP_GUARD_PLEXUS_DETAIL_LOAN_POOL_H

#include "plexus/io/object_carrier.h"

#include "plexus/detail/loan_pool_slab.h"

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

    ~loan()
    {
        release_if_held();
    }

    [[nodiscard]] bool valid() const noexcept
    {
        return m_slot != nullptr;
    }
    explicit operator bool() const noexcept
    {
        return valid();
    }

    T &operator*() const noexcept
    {
        return *object_ptr();
    }
    T *operator->() const noexcept
    {
        return object_ptr();
    }

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
    io::loan_slot *take() noexcept
    {
        return std::exchange(m_slot, nullptr);
    }

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

// A fixed-capacity, alloc-free object pool for T over a loan_slab. Each slot carries an
// io::loan_slot control so a published object rides the zero-serialization lane by reference.
// Exhaustion (no free slot) is NOT an error and NEVER blocks or grows: try_borrow returns an empty
// loan and the publisher degrades to the serialize path with a counter.
template<typename T>
class loan_pool
{
public:
    explicit loan_pool(std::size_t capacity)
            : m_slab(capacity)
    {
    }

    // Borrow a slot and construct a T in place from the forwarded arguments. Returns an empty loan
    // on exhaustion. The slot's refcount starts at 1 (the loan's own reference); a publish
    // transfers that reference to the in-flight carrier.
    template<typename... Args>
    loan<T> try_borrow(Args &&...args)
    {
        auto *n = m_slab.pop_free();
        if(n == nullptr)
            return loan<T>{};
        T *obj            = ::new(static_cast<void *>(&n->storage)) T(std::forward<Args>(args)...);
        n->control.object = obj;
        n->control.refs   = 1;
        return loan<T>{&n->control};
    }

    // Build the carrier for a borrowed loan and surrender the slot to it: the loan is left null
    // (its release obligation passed to the carrier's refcount), the carrier carries the type
    // witness and the live slot. The forwarder addrefs per fast-path send and releases once after
    // the fan loop, so the slot is balanced on every path.
    static io::object_carrier carrier_for(loan<T> &held, std::uint64_t type_tag) noexcept
    {
        io::object_carrier carrier{};
        carrier.type_tag   = type_tag;
        carrier.native_key = &io::detail::type_key<T>;
        carrier.slot       = held.take();
        return carrier;
    }

    [[nodiscard]] std::size_t capacity() const noexcept
    {
        return m_slab.capacity();
    }

private:
    loan_slab<T> m_slab;
};

}

#endif
