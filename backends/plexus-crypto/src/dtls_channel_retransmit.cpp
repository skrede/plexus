#include "plexus/tls/dtls_channel.h"

#include "plexus/tls/detail/dtls_retransmit.h"

#include <openssl/err.h>
#include <openssl/ssl.h>

#include <mutex>
#include <chrono>
#include <cstdint>
#include <system_error>

namespace plexus::tls {

namespace {

std::int32_t retransmit_ceiling_ex_index() noexcept
{
    static std::once_flag once;
    static std::int32_t index = -1;
    std::call_once(once, [] { index = ::SSL_get_ex_new_index(0, nullptr, nullptr, nullptr, nullptr); });
    return index;
}

}

namespace detail {

void install_retransmit_ceiling(ssl_st *ssl, std::chrono::microseconds &ceiling)
{
    ::SSL_set_ex_data(ssl, retransmit_ceiling_ex_index(), &ceiling);
    ::DTLS_set_timer_cb(ssl, plexus_dtls_retransmit_timer_cb);
}

extern "C" unsigned int plexus_dtls_retransmit_timer_cb(ssl_st *ssl, unsigned int timer_us)
{
    const auto *ceiling = static_cast<const std::chrono::microseconds *>(::SSL_get_ex_data(ssl, retransmit_ceiling_ex_index()));
    return next_retransmit_us(timer_us, ceiling ? *ceiling : dtls_channel::default_retransmit_ceiling);
}

}

void dtls_channel::arm_retransmit()
{
    if(!m_ssl || !m_open)
        return;
    struct timeval tv{};
    if(::DTLSv1_get_timeout(m_ssl, &tv) != 1)
        return;
    const auto ms = std::chrono::milliseconds(static_cast<std::int64_t>(tv.tv_sec) * 1000 + static_cast<std::int64_t>(tv.tv_usec) / 1000);
    m_retransmit.cancel();
    m_retransmit.expires_after(ms);
    m_ops.begin();
    m_retransmit.async_wait(
            [this](std::error_code ec)
            {
                m_ops.end();
                if(ec || !m_open)
                    return;
                on_retransmit();
            });
}

void dtls_channel::on_retransmit()
{
    if(!m_ssl || !m_open)
        return;
    const int r = ::DTLSv1_handle_timeout(m_ssl);
    if(r < 0)
    {
        // The expiry is pushed onto the io thread's error queue, where it would outlive this channel.
        ::ERR_clear_error();
        return fail(io::io_error::timed_out);
    }
    if(r > 0)
        drain_outbound();
    arm_retransmit();
}

}
