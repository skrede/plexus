#ifndef HPP_GUARD_PLEXUS_CALL_ERROR_H
#define HPP_GUARD_PLEXUS_CALL_ERROR_H

#include "plexus/wire/rpc_status.h"

#include <string>
#include <cstdint>
#include <system_error>

namespace plexus {

// The caller-facing error vocabulary for a request/response call. Every value but
// no_provider mirrors an rpc_status failure the wire can carry back; no_provider is
// minted at the facade for a call dispatched with no connected provider — a
// pre-wire failure the responder never sees.
enum class call_errc : std::uint8_t
{
    error               = 1,
    timeout             = 2,
    cancelled           = 3,
    no_handler          = 4,
    deserialize_failed  = 5,
    topic_not_found     = 8,
    peer_disconnected   = 18,
    rpc_response_orphan = 20,
    no_provider         = 240,
};

const std::error_category &call_category() noexcept;
const std::error_category &provider_category() noexcept;

inline std::error_code make_error_code(call_errc e) noexcept
{
    return {static_cast<int>(e), call_category()};
}

// Maps a wire rpc_status onto its call_errc. Precondition: status is a failure
// (success has no error to carry); passing success is a contract violation and
// returns error rather than fabricating a non-failure code.
inline call_errc from_rpc_status(wire::rpc_status status) noexcept
{
    switch(status)
    {
        case wire::rpc_status::error:               return call_errc::error;
        case wire::rpc_status::timeout:             return call_errc::timeout;
        case wire::rpc_status::cancelled:           return call_errc::cancelled;
        case wire::rpc_status::no_handler:          return call_errc::no_handler;
        case wire::rpc_status::deserialize_failed:  return call_errc::deserialize_failed;
        case wire::rpc_status::topic_not_found:     return call_errc::topic_not_found;
        case wire::rpc_status::peer_disconnected:   return call_errc::peer_disconnected;
        case wire::rpc_status::rpc_response_orphan: return call_errc::rpc_response_orphan;
        case wire::rpc_status::success:             return call_errc::error;
    }
    return call_errc::error;
}

namespace detail {

class call_error_category final : public std::error_category
{
public:
    [[nodiscard]] const char *name() const noexcept override { return "plexus.call"; }

    [[nodiscard]] std::string message(int value) const override
    {
        switch(static_cast<call_errc>(value))
        {
            case call_errc::error:               return "call failed";
            case call_errc::timeout:             return "call timed out";
            case call_errc::cancelled:           return "call cancelled";
            case call_errc::no_handler:          return "no handler for call";
            case call_errc::deserialize_failed:  return "call payload deserialization failed";
            case call_errc::topic_not_found:     return "call topic not found";
            case call_errc::peer_disconnected:   return "peer disconnected during call";
            case call_errc::rpc_response_orphan: return "call response orphaned";
            case call_errc::no_provider:         return "no connected provider for call";
        }
        return "unknown call error";
    }
};

// Carries a typed provider handler's error VALUE across the wire under a generic
// category: an error_category's identity is its object address, which cannot cross a
// process, so the originating category is erased in transit and only the integer value
// is preserved. A consumer reconstructs intent from the value against its own contract.
class provider_error_category final : public std::error_category
{
public:
    [[nodiscard]] const char *name() const noexcept override { return "plexus.provider"; }

    [[nodiscard]] std::string message(int value) const override
    {
        return "provider error " + std::to_string(value);
    }
};

}

// A function-local static error_category: the std::error_category identity contract
// (one stable address per category, compared by &) is the standard's own requirement,
// a standing exception to the no-static-singleton posture.
inline const std::error_category &call_category() noexcept
{
    static detail::call_error_category category;
    return category;
}

inline const std::error_category &provider_category() noexcept
{
    static detail::provider_error_category category;
    return category;
}

}

template<>
struct std::is_error_code_enum<plexus::call_errc> : std::true_type
{
};

#endif
