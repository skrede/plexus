#ifndef HPP_GUARD_PLEXUS_ASIO_DETAIL_UDP_CHANNEL_POST_H
#define HPP_GUARD_PLEXUS_ASIO_DETAIL_UDP_CHANNEL_POST_H

#include "plexus/wire/frame_reassembler.h"

#include <asio/post.hpp>

#include <span>
#include <memory>
#include <vector>
#include <cstddef>
#include <utility>

namespace plexus::asio::detail {

// on_data is always posted (the byte_channel contract); the owning vector keeps the bytes alive
// across the post (the demux reuses its recv buffer immediately).
template<typename Ch>
void post_on_data(Ch &c, std::span<const std::byte> frame)
{
    if(!c.m_open)
        return;
    c.m_ops.begin();
    auto owned = std::make_shared<std::vector<std::byte>>(frame.begin(), frame.end());
    ::asio::post(c.m_io,
                 [&c, owned]
                 {
                     c.m_ops.end();
                     if(c.m_on_data_cb)
                         c.m_on_data_cb(std::span<const std::byte>{*owned});
                 });
}

// Carry the already-owning bytes straight into the posted closure (no second copy).
template<typename Ch>
void post_on_data_owned(Ch &c, wire::shared_bytes owned)
{
    if(!c.m_open)
        return;
    c.m_ops.begin();
    ::asio::post(c.m_io,
                 [&c, owned = std::move(owned)]
                 {
                     c.m_ops.end();
                     if(c.m_on_data_cb)
                         c.m_on_data_cb(static_cast<std::span<const std::byte>>(owned));
                 });
}

}

#endif
