#ifndef HPP_GUARD_PLEXUS_DETAIL_LOAN_POOL_SLAB_H
#define HPP_GUARD_PLEXUS_DETAIL_LOAN_POOL_SLAB_H

#include "plexus/io/object_carrier.h"

#include <new>
#include <cstddef>
#include <utility>

namespace plexus::detail {

template<typename T>
struct slab_control;

// owner is the back-pointer the static release follows to route a freed slot home: a stable heap
// address (the control block), so it survives a slab move untouched.
template<typename T>
struct loan_slab_node
{
    alignas(T) std::byte storage[sizeof(T)];
    io::loan_slot control;
    slab_control<T> *owner;
    loan_slab_node *next_free;
};

// The heap-resident backing a loan_slab: it owns the slot array and freelist and outlives the
// loan_slab handle whenever slots are still in flight. `outstanding` counts borrowed-not-yet-released
// slots; `pool_alive` records whether a loan_slab handle still refers to the block.
template<typename T>
struct slab_control
{
    using node = loan_slab_node<T>;

    node *slots;
    node *free;
    std::size_t capacity;
    std::size_t outstanding;
    bool pool_alive;

    node *pop() noexcept
    {
        if(free == nullptr)
            return nullptr;
        node *n = free;
        free    = n->next_free;
        ++outstanding;
        return n;
    }

    void push(node *n) noexcept
    {
        n->next_free = free;
        free         = n;
        --outstanding;
    }
};

// The fixed-capacity slab backing a loan_pool. The slot array is allocated once, inside a heap
// control block; thereafter pop/push reuse slots through an intrusive freelist with no further
// allocation. The handle move is an O(1) transfer of the control-block pointer — no per-slot fixup —
// because the slots' owner back-pointers target the (unmoved) control block, not this handle.
template<typename T>
class loan_slab
{
public:
    using node = loan_slab_node<T>;

    explicit loan_slab(std::size_t capacity)
            : m_control(new slab_control<T>{new node[capacity], nullptr, capacity, 0, true})
    {
        for(std::size_t i = 0; i < capacity; ++i)
        {
            node &slot           = m_control->slots[i];
            slot.owner           = m_control;
            slot.control.refs    = 0;
            slot.control.release = &release_slot;
            slot.next_free       = m_control->free;
            m_control->free      = &slot;
        }
    }

    ~loan_slab()
    {
        release_control();
    }

    loan_slab(const loan_slab &)            = delete;
    loan_slab &operator=(const loan_slab &) = delete;

    loan_slab(loan_slab &&other) noexcept
            : m_control(std::exchange(other.m_control, nullptr))
    {
    }

    loan_slab &operator=(loan_slab &&other) noexcept
    {
        if(this != &other)
        {
            release_control();
            m_control = std::exchange(other.m_control, nullptr);
        }
        return *this;
    }

    node *pop_free() noexcept
    {
        return m_control->pop();
    }

    std::size_t capacity() const noexcept
    {
        return m_control->capacity;
    }

private:
    // The node is recovered from its control member's offset, its control block from the back-pointer —
    // T is known statically here, never a cast across an erased boundary.
    static void release_slot(io::loan_slot *control) noexcept
    {
        node *n = reinterpret_cast<node *>(reinterpret_cast<std::byte *>(control) - offsetof(node, control));
        std::launder(reinterpret_cast<T *>(&n->storage))->~T();
        control->object        = nullptr;
        slab_control<T> *owner = n->owner;
        owner->push(n);
        // Defer-free ordering: the storage is freed only once the handle is gone (pool_alive false)
        // AND no slot is in flight (outstanding zero). A publish outstanding across the handle's death
        // reaches here last and frees the block — so ~T()/push never touch freed storage.
        if(owner->outstanding == 0 && !owner->pool_alive)
            destroy_control(owner);
    }

    static void destroy_control(slab_control<T> *c) noexcept
    {
        delete[] c->slots;
        delete c;
    }

    void release_control() noexcept
    {
        if(m_control == nullptr)
            return;
        m_control->pool_alive = false;
        if(m_control->outstanding == 0)
            destroy_control(m_control);
        m_control = nullptr;
    }

    slab_control<T> *m_control;
};

}

#endif
