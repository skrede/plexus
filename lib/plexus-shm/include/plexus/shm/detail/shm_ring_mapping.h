#ifndef HPP_GUARD_PLEXUS_SHM_DETAIL_SHM_RING_MAPPING_H
#define HPP_GUARD_PLEXUS_SHM_DETAIL_SHM_RING_MAPPING_H

#include <memory>

namespace plexus::shm::detail {

// One ring's mappings, kept alive in this process while its entry or any message taken from it holds a share. The
// share is process-local and never enters shared memory.
class ring_mapping
{
public:
    ring_mapping() noexcept = default;
    virtual ~ring_mapping() = default;

    ring_mapping(const ring_mapping &)            = delete;
    ring_mapping &operator=(const ring_mapping &) = delete;
};

using mapping_pin = std::shared_ptr<const ring_mapping>;

template<typename Handle>
class ring_mapping_of final : public ring_mapping
{
public:
    ring_mapping_of()
            : control()
            , slab()
    {
    }

    Handle control;
    Handle slab;
};

}

#endif
