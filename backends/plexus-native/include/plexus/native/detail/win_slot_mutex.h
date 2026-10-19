#ifndef HPP_GUARD_PLEXUS_NATIVE_DETAIL_WIN_SLOT_MUTEX_H
#define HPP_GUARD_PLEXUS_NATIVE_DETAIL_WIN_SLOT_MUTEX_H

#include "plexus/shm/slot_liveness.h"

#include <string>
#include <cstdint>

namespace plexus::native::detail {

// A slot's object is named <canonical>.<slot> after its ring's broker name; the HANDLE travels as void* to keep
// <windows.h> out of this header. On held, `out_mutex` is the holder's only long-lived handle.
plexus::shm::slot_hold create_slot_mutex(const std::string &canonical, std::uint32_t slot, void *&out_mutex);

void close_slot_mutex(void *&mutex) noexcept;

// True as well when the object exists but cannot be opened, so a caller never declares a holder dead without proof.
bool slot_mutex_exists(const std::string &canonical, std::uint32_t slot);

}

#endif
