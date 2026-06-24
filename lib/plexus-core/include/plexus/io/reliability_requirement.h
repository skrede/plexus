#ifndef HPP_GUARD_PLEXUS_IO_RELIABILITY_REQUIREMENT_H
#define HPP_GUARD_PLEXUS_IO_RELIABILITY_REQUIREMENT_H

#include <cstdint>
#include <string_view>

namespace plexus::io {

enum class reliability_requirement : std::uint8_t
{
    any,
    reliable,
};

// The engine-side mirror of the asio selector's reliability_of_scheme; MUST stay
// consistent with it. Fail-closed: plain "udp" and any unrecognized scheme are NOT
// reliable, so a strict-reliable demand is never admitted over an unproven transport.
inline bool scheme_is_reliable(std::string_view scheme) noexcept
{
    return scheme == "udpr" || scheme == "tcp" || scheme == "tls" || scheme == "unix" || scheme == "inproc";
}

}

#endif
