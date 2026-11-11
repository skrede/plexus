#ifndef HPP_GUARD_PLEXUS_TLS_DETAIL_DTLS_RETRANSMIT_H
#define HPP_GUARD_PLEXUS_TLS_DETAIL_DTLS_RETRANSMIT_H

#include "plexus/detail/fail_closed.h"

#include <chrono>
#include <limits>
#include <cstdint>
#include <algorithm>

struct ssl_st;

namespace plexus::tls::detail {

// RFC 6347 section 4.2.4.1: the handshake retransmit timer starts at 1 s and doubles at each retransmission.
inline constexpr std::uint32_t k_first_retransmit_us = 1'000'000;

// OpenSSL's dtls1_get_timeout treats under 15 ms left as already expired, so a shorter interval fires every
// retransmit back to back inside one SSL call.
inline constexpr std::chrono::microseconds k_min_retransmit_ceiling{15'000};

inline std::chrono::microseconds checked_retransmit_ceiling(std::chrono::microseconds ceiling)
{
    if(ceiling < k_min_retransmit_ceiling)
        plexus::detail::fail_closed("dtls: the retransmit ceiling must be at least 15 ms");
    return ceiling;
}

// OpenSSL passes 0 when a timer starts and the last interval on each expiry. It carries the interval as an unsigned int
// of microseconds, so the ceiling is itself held to that range.
inline std::uint32_t next_retransmit_us(std::uint32_t previous_us, std::chrono::microseconds ceiling) noexcept
{
    const std::uint64_t cap  = std::min<std::uint64_t>(static_cast<std::uint64_t>(ceiling.count()), std::numeric_limits<std::uint32_t>::max());
    const std::uint64_t next = previous_us == 0 ? std::uint64_t{k_first_retransmit_us} : std::uint64_t{previous_us} * 2;
    return static_cast<std::uint32_t>(std::min(next, cap));
}

void install_retransmit_ceiling(ssl_st *ssl, std::chrono::microseconds &ceiling);

extern "C" unsigned int plexus_dtls_retransmit_timer_cb(ssl_st *ssl, unsigned int timer_us);

}

#endif
