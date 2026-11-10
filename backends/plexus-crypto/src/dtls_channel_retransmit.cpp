#include "plexus/tls/dtls_channel.h"

#include <openssl/ssl.h>

#include <chrono>
#include <cstdint>
#include <system_error>

namespace plexus::tls {

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
    if(r > 0)
        drain_outbound();
    if(r >= 0)
        arm_retransmit();
}

}
