#ifndef HPP_GUARD_TESTS_INTEGRATION_SERIAL_RX_PROBE_H
#define HPP_GUARD_TESTS_INTEGRATION_SERIAL_RX_PROBE_H

#include "test_serial_common.h"

#include "support/serial_bytes.h"

#include "plexus/io/frame_router.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/data_frame.h"
#include "plexus/wire/close_cause.h"

#include "plexus/detail/compat.h"

#include <catch2/catch_test_macros.hpp>

#include <unistd.h>

#include <span>
#include <chrono>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace serial_fixture {

inline void write_raw(int fd, std::span<const std::byte> bytes)
{
    REQUIRE(::write(fd, bytes.data(), bytes.size()) == static_cast<ssize_t>(bytes.size()));
}

inline std::vector<std::byte> serial_data_frame(const std::string &payload, std::uint64_t session_id)
{
    return plexus::testing::serial_bytes(make_data_frame(payload, session_id));
}

struct rx_delivery
{
    std::chrono::steady_clock::time_point at;
    std::size_t data_bytes;
};

struct rx_probe
{
    rx_probe()
            : closed(false)
            , drops(0)
            , protocol_closed(false)
            , sink()
            , router(sink)
            , deliveries()
            , received()
            , delivered_cb()
    {
    }

    void wire(pasio::serial_channel &ch)
    {
        router.on_unidirectional([this](const wire::frame_header &, std::span<const std::byte> inner) { deliver(inner); });
        ch.on_data([this](std::span<const std::byte> f) { router.route(f); });
        ch.on_frame_dropped(
                [this](wire::close_cause c)
                {
                    if(c == wire::close_cause::crc_mismatch)
                        ++drops;
                });
        ch.on_protocol_close([this](wire::close_cause) { protocol_closed = true; });
        ch.on_closed([this] { closed = true; });
    }

    bool closed;
    std::int32_t drops;
    bool protocol_closed;
    plexus::log::null_logger sink;
    pio::frame_router router;
    std::vector<rx_delivery> deliveries;
    std::optional<std::string> received;
    plexus::detail::move_only_function<void()> delivered_cb;

private:
    void deliver(std::span<const std::byte> inner)
    {
        const auto decoded = wire::decode_unidirectional(inner);
        if(!decoded)
            return;
        received = to_string(decoded->data);
        deliveries.push_back(rx_delivery{std::chrono::steady_clock::now(), decoded->data.size()});
        if(delivered_cb)
            delivered_cb();
    }
};

}

#endif
