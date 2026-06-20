#ifndef HPP_GUARD_PLEXUS_IO_SHM_SHM_SLOT_OWNER_H
#define HPP_GUARD_PLEXUS_IO_SHM_SHM_SLOT_OWNER_H

#include <atomic>
#include <cstdint>
#include <utility>

namespace plexus::io::shm {

// The move-only, intrusive wire_bytes owner that pins a ring slot's take_refcount
// for the lifetime of a deserialized view (the loan reshape: no reference-counted
// heap handle). It is the SHM-local owner the zero-copy take() path selects for
// its returned wire_bytes<shm_slot_owner>; the universal default wire_bytes owner
// of the other policies (the type-erased heap handle) is UNCHANGED.
//
// It pins on construction (fetch_add) and unpins EXACTLY ONCE on destruction
// (fetch_sub), so the slot the view aliases is never recycled by a best-effort
// overwrite while the view is alive. Copy is deleted; move steals the back-pointer
// and nulls the source so a moved-from owner's destructor no-ops. A
// default-constructed (null) owner pins and unpins nothing — it is the empty
// wire_bytes default-owner case.
class shm_slot_owner
{
public:
    shm_slot_owner() = default;

    explicit shm_slot_owner(std::atomic<std::uint32_t> *refcount) noexcept
            : m_rc(refcount)
    {
        if(m_rc != nullptr)
            m_rc->fetch_add(1, std::memory_order_acq_rel); // pin
    }

    ~shm_slot_owner() { release(); }

    shm_slot_owner(const shm_slot_owner &)            = delete;
    shm_slot_owner &operator=(const shm_slot_owner &) = delete;

    shm_slot_owner(shm_slot_owner &&other) noexcept
            : m_rc(std::exchange(other.m_rc, nullptr))
    {
    }

    shm_slot_owner &operator=(shm_slot_owner &&other) noexcept
    {
        if(this != &other)
        {
            release();
            m_rc = std::exchange(other.m_rc, nullptr);
        }
        return *this;
    }

private:
    void release() noexcept
    {
        if(m_rc != nullptr)
            m_rc->fetch_sub(1, std::memory_order_acq_rel); // unpin once
        m_rc = nullptr;
    }

    std::atomic<std::uint32_t> *m_rc{nullptr};
};

}

#endif
