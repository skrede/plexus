#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_ORDER_FORWARDER_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_ORDER_FORWARDER_H

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/message_forwarder.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/companion.h"
#include "plexus/wire/data_frame.h"
#include "plexus/wire/close_cause.h"
#include "plexus/wire/frame_codec.h"

#include "plexus/wire/detail/data_frame_codec.h"

#include "plexus/policy.h"
#include "plexus/detail/compat.h"

#include "plexus/testing/inline_verbs.h"

#include <span>
#include <chrono>
#include <memory>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <functional>
#include <system_error>

namespace plexus::testing {

using arrival_log = std::vector<std::string>;

// A data frame's body, "subscribe" for a subscribe request, and "" for every other control frame.
inline std::string arrival_of(std::span<const std::byte> frame)
{
    const auto header = plexus::wire::decode_header(frame);
    if(header && header->type == plexus::wire::msg_type::subscribe)
        return "subscribe";
    if(!header || header->type != plexus::wire::msg_type::unidirectional)
        return {};
    const auto data = plexus::wire::decode_unidirectional(frame.subspan(plexus::wire::header_size));
    return data ? std::string{reinterpret_cast<const char *>(data->data.data()), data->data.size()} : std::string{};
}

// The first sequence a switch marker names; nullopt for any other frame.
inline std::optional<std::uint64_t> switch_of(std::span<const std::byte> frame)
{
    const auto header = plexus::wire::decode_header(frame);
    if(!header || header->type != plexus::wire::msg_type::companion_switch)
        return std::nullopt;
    const auto marker = plexus::wire::decode_companion_switch(frame.subspan(plexus::wire::header_size));
    return marker ? std::optional<std::uint64_t>{marker->first_sequence} : std::nullopt;
}

struct carried_frame
{
    std::string body;
    std::uint64_t sequence;
};

// A banded channel whose backpressure the test controls; it appends every frame it is handed to a log both channels
// share, so the log reads in the order the subscriber would see the frames.
struct order_channel
{
    order_channel(arrival_log &log, std::size_t &occupancy, std::uint64_t key)
            : m_key(key)
            , m_carried(0)
            , m_log(log)
            , m_occupancy(occupancy)
            , m_frames()
            , m_switches()
    {
    }

    void send(std::span<const std::byte> frame)
    {
        if(const std::optional<std::uint64_t> first = switch_of(frame))
            m_switches.push_back(*first);
        if(std::string arrival = arrival_of(frame); !arrival.empty())
        {
            m_log.get().push_back(arrival);
            m_frames.push_back({std::move(arrival), plexus::wire::framed_sequence(frame).value_or(0)});
            ++m_carried;
        }
    }

    std::size_t carried() const noexcept
    {
        return m_carried;
    }

    // What this channel carried, in order, and the first sequence of each switch marker sent on it.
    const std::vector<carried_frame> &frames() const noexcept
    {
        return m_frames;
    }

    const std::vector<std::uint64_t> &switches() const noexcept
    {
        return m_switches;
    }

    void close()
    {
    }

    plexus::io::endpoint remote_endpoint() const
    {
        return {"tcp", "127.0.0.1:0"};
    }

    void on_data(plexus::detail::move_only_function<void(std::span<const std::byte>)>)
    {
    }

    void on_closed(plexus::detail::move_only_function<void()>)
    {
    }

    void on_error(plexus::detail::move_only_function<void(plexus::io::io_error)>)
    {
    }

    void on_protocol_close(plexus::detail::move_only_function<void(plexus::wire::close_cause)>)
    {
    }

    std::size_t backpressured() const noexcept
    {
        return m_occupancy.get();
    }

    std::uint64_t scheduler_key() const noexcept
    {
        return m_key;
    }

private:
    std::uint64_t m_key;
    std::size_t m_carried;
    std::reference_wrapper<arrival_log> m_log;
    std::reference_wrapper<std::size_t> m_occupancy;
    std::vector<carried_frame> m_frames;
    std::vector<std::uint64_t> m_switches;
};

struct order_executor
{
};

struct order_timer
{
    explicit order_timer(order_executor &)
    {
    }

    order_timer(order_executor &, std::error_code &)
    {
    }

    void expires_after(std::chrono::milliseconds)
    {
    }

    void async_wait(plexus::detail::move_only_function<void(std::error_code)>)
    {
    }

    void cancel()
    {
    }
};

struct order_policy : plexus::testing::inline_verbs<order_executor &>
{
    using byte_channel_type = order_channel;
    using timer_type        = order_timer;
    using byte_owner        = std::shared_ptr<const void>;
};

static_assert(plexus::Policy<order_policy>);

using order_forwarder = plexus::io::message_forwarder<order_policy>;

inline std::span<const std::byte> as_bytes(const std::string &s)
{
    return {reinterpret_cast<const std::byte *>(s.data()), s.size()};
}

}

#endif
