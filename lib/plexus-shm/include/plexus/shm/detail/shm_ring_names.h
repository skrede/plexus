#ifndef HPP_GUARD_PLEXUS_SHM_DETAIL_SHM_RING_NAMES_H
#define HPP_GUARD_PLEXUS_SHM_DETAIL_SHM_RING_NAMES_H

#include "plexus/shm/ring_header.h"
#include "plexus/shm/region_broker_concept.h"

#include <new>
#include <span>
#include <atomic>
#include <string>
#include <cstddef>
#include <cstdint>

namespace plexus::shm::detail {

// A ring's names are removed only by a holder of its retired region's creator slot or of every slot, once a fresh attach
// of the control name reads that region's identity. The slab name goes first, whatever it holds, then the control name,
// and its guard goes after it.
struct ring_names
{
    // Null for a mapping too small to hold a ring header.
    static control_header_t *header_of(std::span<std::byte> control) noexcept
    {
        if(control.size() < sizeof(control_header_t))
            return nullptr;
        return std::launder(reinterpret_cast<control_header_t *>(control.data()));
    }

    // A mapping too small for a header reads as a magic no ring carries.
    static std::uint64_t magic_of(std::span<std::byte> control) noexcept
    {
        const control_header_t *header = header_of(control);
        return header == nullptr ? ~std::uint64_t{0} : header->magic.load(std::memory_order_acquire);
    }

    // A mapping too small for a header reads as a ring without an identity.
    static std::uint64_t identity_of(std::span<std::byte> control) noexcept
    {
        const control_header_t *header = header_of(control);
        return header == nullptr ? 0 : header->identity.load(std::memory_order_relaxed);
    }

    // A ring without an identity names no ring provably.
    template<typename Broker>
    static bool names_ring(Broker &broker, const std::string &ctrl_name, std::uint64_t identity)
    {
        typename Broker::region_handle fresh;
        return identity != 0 && broker.attach(ctrl_name, fresh) == region_status::ok && identity_of(fresh.bytes()) == identity;
    }

    template<typename Broker, typename Entry>
    static bool remove_names(Broker &broker, Entry &e, const std::string &ctrl_name)
    {
        if(!names_ring(broker, ctrl_name, identity_of(e.control.bytes())))
            return false;
        typename Broker::region_handle slab;
        if(broker.attach(ctrl_name + ".s", slab) == region_status::ok)
            slab.unlink_name();
        e.control.unlink_name();
        return true;
    }
};

}

#endif
