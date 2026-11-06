#ifndef HPP_GUARD_PLEXUS_TESTS_SEAM_LWIP_RX_RIG_H
#define HPP_GUARD_PLEXUS_TESTS_SEAM_LWIP_RX_RIG_H

#if !defined(ESP_PLATFORM)

    // host_tcp_socket.h declares the host stream_socket the freertos headers are instantiated over, so it comes first.
    #include "host_tcp_socket.h"
    #include "host_loopback_pair.h"

    #include "plexus/freertos/lwip_channel.h"
    #include "plexus/freertos/lwip_rx_task.h"
    #include "plexus/freertos/freertos_executor.h"

    #include "plexus/wire/frame.h"
    #include "plexus/wire/frame_codec.h"

    #include <span>
    #include <atomic>
    #include <chrono>
    #include <memory>
    #include <thread>
    #include <vector>
    #include <cstddef>
    #include <utility>
    #include <optional>

namespace plexus::test {

using rx_channel = plexus::freertos::lwip_channel<host_tcp_socket>;
using rx_ctx     = plexus::freertos::detail::lwip_rx_ctx<host_tcp_socket>;

inline constexpr std::chrono::milliseconds k_recv_timeout{100};

inline host_tcp_socket parked_receiver(host_tcp_socket socket, std::atomic<std::thread::id> &closer)
{
    socket.set_blocking(true);
    socket.set_receive_timeout(k_recv_timeout);
    socket.use_close_witness(closer);
    return socket;
}

// The host xTaskCreate records the trampoline and its argument instead of running them.
inline rx_ctx &spawned_ctx()
{
    return *static_cast<rx_ctx *>(plexus::freertos::detail::host_task_created_arg);
}

inline void run_spawned_task()
{
    plexus::freertos::detail::host_task_created_code(plexus::freertos::detail::host_task_created_arg);
}

inline void release_spawned_ctx()
{
    delete &spawned_ctx();
}

inline std::size_t acquirable_slots(rx_channel &channel)
{
    std::vector<rx_channel::rx_slot *> taken;
    while(auto *slot = channel.rx().acquire(0))
        taken.push_back(slot);
    for(auto *slot : taken)
        channel.rx().release(*slot);
    return taken.size();
}

inline std::vector<std::byte> on_wire(std::span<const std::byte> payload)
{
    return plexus::wire::encode_frame(plexus::wire::frame_header{.type = plexus::wire::msg_type::unidirectional, .flags = 0, .session_id = 0, .timestamp_ns = 0, .payload_len = 0},
                                      payload);
}

struct rx_rig
{
    rx_rig()
            : closer()
            , ex()
            , pair(make_loopback_pair())
            , sender(std::move(pair.value().dialed))
            , channel(std::make_unique<rx_channel>(parked_receiver(std::move(pair.value().accepted), closer), ex, plexus::io::endpoint{"tcp", "127.0.0.1:0"}))
    {
    }

    BaseType_t spawn()
    {
        return plexus::freertos::spawn_lwip_rx_task(*channel, ex, plexus::freertos::task_options{4096});
    }

    std::atomic<std::thread::id> closer;
    plexus::freertos::freertos_executor ex;
    std::optional<loopback_pair> pair;
    host_tcp_socket sender;
    std::unique_ptr<rx_channel> channel;
};

}

#endif

#endif
