#ifndef HPP_GUARD_PLEXUS_DETAIL_NODE_OPTIONS_RESOLVE_H
#define HPP_GUARD_PLEXUS_DETAIL_NODE_OPTIONS_RESOLVE_H

#include "plexus/detail/fail_closed.h"

#include "plexus/node_id.h"
#include "plexus/node_options.h"

#include "plexus/io/reconnect_config.h"

#include <chrono>
#include <cstddef>
#include <cstdint>

namespace plexus::detail {

inline const io::reconnect_config &checked_reconnect(const node_options &opts)
{
    const io::reconnect_config &cfg = opts.reconnect;
    if(cfg.min_delay <= std::chrono::milliseconds::zero() || cfg.max_delay < cfg.min_delay)
        fail_closed("node_options: reconnect min_delay must be positive and max_delay at least min_delay");
    return cfg;
}

// An unset seed is the 64-bit FNV-1a of the id's bytes.
inline std::uint64_t resolved_redial_seed(const node_id &id, const node_options &opts) noexcept
{
    if(opts.redial_seed)
        return *opts.redial_seed;
    constexpr std::uint64_t k_prime = 1099511628211ull;
    std::uint64_t hash              = 14695981039346656037ull;
    for(std::byte b : id)
        hash = (hash ^ std::to_integer<std::uint64_t>(b)) * k_prime;
    return hash;
}

}

#endif
