#include "plexus/tls/detail/dtls_identity.h"

#include <cstdint>

namespace plexus::tls::detail {

std::vector<unsigned char> pack_peer_addr(const ::asio::ip::udp::endpoint &ep)
{
    std::vector<unsigned char> block;
    const auto                 addr = ep.address();
    std::vector<unsigned char> raw;
    if(addr.is_v4())
    {
        auto b = addr.to_v4().to_bytes();
        raw.assign(b.begin(), b.end());
    }
    else
    {
        auto b = addr.to_v6().to_bytes();
        raw.assign(b.begin(), b.end());
    }
    const std::uint16_t port = ep.port();
    raw.push_back(static_cast<unsigned char>(port >> 8));
    raw.push_back(static_cast<unsigned char>(port & 0xff));

    block.reserve(raw.size() + 1);
    block.push_back(static_cast<unsigned char>(raw.size()));
    block.insert(block.end(), raw.begin(), raw.end());
    return block;
}

}
