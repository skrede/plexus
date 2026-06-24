#include "plexus/native/machine_fingerprint.h"

#include "plexus/wire/topic_hash.h"

#include <fstream>
#include <string>

#include <unistd.h>

namespace plexus::native {

namespace {

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
