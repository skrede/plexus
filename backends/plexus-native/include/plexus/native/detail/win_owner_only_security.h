#ifndef HPP_GUARD_PLEXUS_NATIVE_DETAIL_WIN_OWNER_ONLY_SECURITY_H
#define HPP_GUARD_PLEXUS_NATIVE_DETAIL_WIN_OWNER_ONLY_SECURITY_H

#include <windows.h>
#include <sddl.h>

namespace plexus::native::detail {

// SDDL "D:P(A;;GA;;;OW)": a protected DACL granting all access to the object owner
// only (the 0600 analog); the creator token becomes the owner. get() yields the
// SECURITY_ATTRIBUTES to hand CreateFileMappingW, or nullptr if construction failed.
class win_owner_only_security
{
public:
    win_owner_only_security()
            : m_descriptor(nullptr)
            , m_attributes{}
    {
        if(::ConvertStringSecurityDescriptorToSecurityDescriptorW(L"D:P(A;;GA;;;OW)", SDDL_REVISION_1, &m_descriptor, nullptr))
        {
            m_attributes.nLength              = sizeof(m_attributes);
            m_attributes.lpSecurityDescriptor = m_descriptor;
            m_attributes.bInheritHandle       = FALSE;
        }
    }

    ~win_owner_only_security()
    {
        if(m_descriptor != nullptr)
            ::LocalFree(m_descriptor);
    }

    win_owner_only_security(const win_owner_only_security &)            = delete;
    win_owner_only_security &operator=(const win_owner_only_security &) = delete;

    SECURITY_ATTRIBUTES *get() noexcept
    {
        return m_descriptor != nullptr ? &m_attributes : nullptr;
    }

private:
    PSECURITY_DESCRIPTOR m_descriptor;
    SECURITY_ATTRIBUTES m_attributes;
};

}

#endif
