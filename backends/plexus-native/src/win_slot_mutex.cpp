#include "plexus/native/detail/win_slot_mutex.h"
#include "plexus/native/detail/win_owner_only_security.h"

#include <string>

#include <windows.h>

namespace plexus::native::detail {

namespace {

// The canonical name is pure ASCII, so a direct widening is sufficient.
std::wstring slot_name(const std::string &canonical, std::uint32_t slot)
{
    std::wstring name(canonical.begin(), canonical.end());
    name += L'.';
    name += std::to_wstring(slot);
    return name;
}

}

// A named kernel object is destroyed when its last handle closes, and the kernel closes every handle of a process that
// exits (learn.microsoft.com, CreateMutexW and "Object Names"): the mutex's existence, not its ownership, is the proof,
// so nothing waits on it and no thread affinity enters.
plexus::shm::slot_hold create_slot_mutex(const std::string &canonical, std::uint32_t slot, void *&out_mutex)
{
    win_owner_only_security security;
    HANDLE h = ::CreateMutexW(security.get(), FALSE, slot_name(canonical, slot).c_str());
    if(h == nullptr)
        return ::GetLastError() == ERROR_ACCESS_DENIED ? plexus::shm::slot_hold::busy : plexus::shm::slot_hold::failed;
    if(::GetLastError() == ERROR_ALREADY_EXISTS)
    {
        ::CloseHandle(h);
        return plexus::shm::slot_hold::busy;
    }
    out_mutex = h;
    return plexus::shm::slot_hold::held;
}

void close_slot_mutex(void *&mutex) noexcept
{
    if(mutex != nullptr)
        ::CloseHandle(static_cast<HANDLE>(mutex));
    mutex = nullptr;
}

// The probe's handle is closed at once so it never keeps a dead holder's object alive past this call.
bool slot_mutex_exists(const std::string &canonical, std::uint32_t slot)
{
    HANDLE h = ::OpenMutexW(SYNCHRONIZE, FALSE, slot_name(canonical, slot).c_str());
    if(h == nullptr)
        return ::GetLastError() != ERROR_FILE_NOT_FOUND;
    ::CloseHandle(h);
    return true;
}

}
