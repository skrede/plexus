#include "plexus/native/machine_fingerprint.h"

#include "plexus/wire/topic_hash.h"

#include <string>

#if defined(_WIN32)
    #include <windows.h>
#else
    #if !defined(__APPLE__)
        #include <fstream>
    #endif

    #include <unistd.h>
#endif

namespace plexus::native {

namespace {

#if defined(_WIN32)

// The per-install machine GUID under HKLM\SOFTWARE\Microsoft\Cryptography — the
// Windows analog of /etc/machine-id. Empty when unreadable.
std::string read_machine_id()
{
    char buf[128] = {};
    DWORD length  = sizeof(buf);
    if(::RegGetValueA(HKEY_LOCAL_MACHINE, "SOFTWARE\\Microsoft\\Cryptography", "MachineGuid", RRF_RT_REG_SZ, nullptr, buf, &length) != ERROR_SUCCESS)
        return {};
    return std::string(buf);
}

// GetComputerNameA (not gethostname, which needs a prior WSAStartup) — WSAStartup-free
// and sufficient here, since the machine GUID already carries the stable host identity.
std::string read_host_name()
{
    char buf[MAX_COMPUTERNAME_LENGTH + 1] = {};
    DWORD length                          = sizeof(buf);
    if(::GetComputerNameA(buf, &length) == 0)
        return {};
    return std::string(buf, length);
}

#elif defined(__APPLE__)

// macOS exposes no /etc/machine-id; a hostname-only seed is the minimum viable
// fingerprint. The FNV-1a path tolerates an empty machine-id, and an IOKit
// IOPlatformUUID enrichment is an on-host follow-up.
std::string read_machine_id()
{
    return {};
}

// The host name (gethostname), empty on failure.
std::string read_host_name()
{
    char buf[256] = {};
    if(::gethostname(buf, sizeof(buf) - 1) != 0)
        return {};
    return std::string(buf);
}

#else

// The kernel host id, e.g. /etc/machine-id on Linux. Empty when unreadable.
std::string read_machine_id()
{
    std::ifstream in("/etc/machine-id");
    if(!in)
        return {};
    std::string id;
    std::getline(in, id);
    return id;
}

// The host name (gethostname), empty on failure.
std::string read_host_name()
{
    char buf[256] = {};
    if(::gethostname(buf, sizeof(buf) - 1) != 0)
        return {};
    return std::string(buf);
}

#endif

}

plexus::io::host_fingerprint read_machine_fingerprint()
{
    // Concatenate machine-id + hostname under a unit-separator (0x1f cannot occur
    // in either field) so two distinct (id, host) splits never alias, then hash
    // through the stable FNV-1a the wire identity already uses. A zero result is
    // never produced for a non-empty input (FNV-1a's offset basis is non-zero and
    // the multiply is invertible), so the fingerprint is non-null on a real host.
    std::string keyed = read_machine_id();
    keyed.push_back('\x1f');
    keyed += read_host_name();
    return plexus::io::host_fingerprint{plexus::wire::fqn_topic_hash(keyed)};
}

}
