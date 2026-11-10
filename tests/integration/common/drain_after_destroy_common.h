#ifndef HPP_GUARD_PLEXUS_TESTS_INTEGRATION_DRAIN_AFTER_DESTROY_COMMON_H
#define HPP_GUARD_PLEXUS_TESTS_INTEGRATION_DRAIN_AFTER_DESTROY_COMMON_H

#include "plexus/wire/frame.h"
#include "plexus/wire/frame_codec.h"

#include <asio/io_context.hpp>

#include <chrono>
#include <vector>
#include <cstddef>
#include <cstdint>

namespace drain_after_destroy {

inline constexpr std::int32_t k_iterations = 8;

// Two complete frames in one buffer, so the single read that carries them posts two deliveries.
inline std::vector<std::byte> two_frames()
{
    plexus::wire::frame_header hdr{};
    hdr.type = plexus::wire::msg_type::unidirectional;
    const std::vector<std::byte> body(16, std::byte{0x42});
    auto out    = plexus::wire::encode_frame(hdr, body);
    auto second = plexus::wire::encode_frame(hdr, body);
    out.insert(out.end(), second.begin(), second.end());
    return out;
}

// One handler per turn, so a test can stop between a completion and the delivery it posted.
template<typename Pred>
bool poll_one_until(::asio::io_context &io, Pred pred)
{
    const auto bound = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while(!pred() && std::chrono::steady_clock::now() < bound)
        io.poll_one();
    return pred();
}

// A poll that ran out of work stops the context, so the drain restarts it before running what the retire posted.
inline void drain(::asio::io_context &io)
{
    io.restart();
    io.poll();
    io.run_for(std::chrono::milliseconds(50));
}

}

#endif
