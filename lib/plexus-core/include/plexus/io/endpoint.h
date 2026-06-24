#ifndef HPP_GUARD_PLEXUS_IO_ENDPOINT_H
#define HPP_GUARD_PLEXUS_IO_ENDPOINT_H

#include <string>

namespace plexus::io {

struct endpoint
{
    std::string scheme;
    std::string address;
};

inline bool operator==(const endpoint &a, const endpoint &b) noexcept
{
    return a.scheme == b.scheme && a.address == b.address;
}

inline bool operator!=(const endpoint &a, const endpoint &b) noexcept
{
    return !(a == b);
}

}

#endif
