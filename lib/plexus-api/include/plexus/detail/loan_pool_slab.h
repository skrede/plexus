#ifndef HPP_GUARD_PLEXUS_DETAIL_LOAN_POOL_SLAB_H
#define HPP_GUARD_PLEXUS_DETAIL_LOAN_POOL_SLAB_H

#include "plexus/io/object_carrier.h"

#include <new>
#include <cstddef>
#include <utility>

namespace plexus::detail {

template<typename T>
class loan_slab;

// One pool slot: aligned storage for a T, its io::loan_slot refcount control, a back-pointer to
// the owning slab (so the static release routes a freed slot home), and the intrusive freelist
// link.
template<typename T>
struct loan_slab_node
{
    alignas(T) std::byte storage[sizeof(T)];
    io::loan_slot   control;
    loan_slab<T>   *owner;
    loan_slab_node *next_free;
};

// The fixed-capacity slab backing a loan_pool: the slot array (the ONLY allocation) is built once
// at construction; thereafter pop/push reuse slots through an intrusive freelist with no further
// allocation. The slab owns the array and the freelist head; a move steals both by pointer and
// re-stamps every slot's owner so a later release routes home (a stale owner would push onto a
// dead slab — a use-after-free).
template<typename T>
class loan_slab
{
public:
    using node = loan_slab_node<T>;

    explicit loan_slab(std::size_t capacity)
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

    ~loan_slab() { delete[] m_slots; }

    loan_slab(const loan_slab &)            = delete;
    loan_slab &operator=(const loan_slab &) = delete;

    loan_slab(loan_slab &&other) noexcept
            : m_slots(std::exchange(other.m_slots, nullptr))
            , m_free(std::exchange(other.m_free, nullptr))
            , m_capacity(std::exchange(other.m_capacity, 0))
    {
        restamp_owner();
    }

    loan_slab &operator=(loan_slab &&other) noexcept
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

    [[nodiscard]] std::size_t capacity() const noexcept { return m_capacity; }

private:
    // Destroy the object in place and return its slot to the freelist. The node is recovered from
    // the control member's offset, and its slab from the back-pointer stamped at build — the
    // producer's concrete type is known statically here, never a cast across an erased boundary.
    static void release_slot(io::loan_slot *control) noexcept
    {
        node *n = reinterpret_cast<node *>(reinterpret_cast<std::byte *>(control) -
                                           offsetof(node, control));
        std::launder(reinterpret_cast<T *>(&n->storage))->~T();
        control->object = nullptr;
        n->owner->push_free(n);
    }

    void restamp_owner() noexcept
    {
        for(std::size_t i = 0; i < m_capacity; ++i)
            m_slots[i].owner = this;
    }

    node       *m_slots;
    node       *m_free{};
    std::size_t m_capacity;
};

}

#endif
