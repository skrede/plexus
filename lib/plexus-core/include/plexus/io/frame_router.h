#ifndef HPP_GUARD_PLEXUS_IO_FRAME_ROUTER_H
#define HPP_GUARD_PLEXUS_IO_FRAME_ROUTER_H

#include "plexus/detail/compat.h"

#include "plexus/io/null_logger.h"

#include "plexus/log/logger.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/frame_codec.h"

#include <span>
#include <cstddef>
#include <utility>
#include <string_view>

namespace plexus::io {

class frame_router
{
public:
    using consumer      = plexus::detail::move_only_function<void(std::span<const std::byte>)>;
    using data_consumer = plexus::detail::move_only_function<void(const wire::frame_header &, std::span<const std::byte>)>;

    explicit frame_router(log::logger &logger) noexcept
            : m_logger(logger)
    {
    }

    void on_unidirectional(data_consumer c)
    {
        m_unidirectional = std::move(c);
    }
    void on_subscribe(consumer c)
    {
        m_subscribe = std::move(c);
    }
    void on_fetch_latched(consumer c)
    {
        m_fetch_latched = std::move(c);
    }
    void on_unsubscribe(consumer c)
    {
        m_unsubscribe = std::move(c);
    }
    void on_subscribe_response(consumer c)
    {
        m_subscribe_response = std::move(c);
    }
    void on_rpc_request(consumer c)
    {
        m_rpc_request = std::move(c);
    }
    void on_rpc_response(consumer c)
    {
        m_rpc_response = std::move(c);
    }
    void on_handshake_req(consumer c)
    {
        m_handshake_req = std::move(c);
    }
    void on_handshake_resp(consumer c)
    {
        m_handshake_resp = std::move(c);
    }
    void on_heartbeat(consumer c)
    {
        m_heartbeat = std::move(c);
    }
    void on_declare(consumer c)
    {
        m_declare = std::move(c);
    }

    // Decode the header, switch on its type, hand the inner payload to the registered consumer.
    // A short/bad-magic frame or an unknown/unregistered type is warn-and-dropped.
    void route(std::span<const std::byte> frame)
    {
        auto hdr = wire::decode_header(frame);
        if(!hdr)
            return drop("plexus: router frame_decode_failed");

        auto inner = frame.subspan(wire::header_size);
        dispatch(*hdr, inner);
    }

private:
    // NOLINTNEXTLINE(readability-function-size)
    void dispatch(const wire::frame_header &hdr, std::span<const std::byte> inner)
    {
        switch(hdr.type)
        {
            case wire::msg_type::unidirectional:
                return fire_data(hdr, inner);
            case wire::msg_type::subscribe:
                return fire(m_subscribe, inner);
            case wire::msg_type::fetch_latched:
                return fire(m_fetch_latched, inner);
            case wire::msg_type::unsubscribe:
                return fire(m_unsubscribe, inner);
            case wire::msg_type::subscribe_response:
                return fire(m_subscribe_response, inner);
            case wire::msg_type::rpc_request:
                return fire(m_rpc_request, inner);
            case wire::msg_type::rpc_response:
                return fire(m_rpc_response, inner);
            case wire::msg_type::handshake_req:
                return fire(m_handshake_req, inner);
            case wire::msg_type::handshake_resp:
                return fire(m_handshake_resp, inner);
            case wire::msg_type::heartbeat:
                return fire(m_heartbeat, inner);
            case wire::msg_type::declare:
                return fire(m_declare, inner);
            default:
                return drop("plexus: router unknown_frame_type");
        }
    }

    void fire(consumer &c, std::span<const std::byte> inner)
    {
        if(!c)
            return drop("plexus: router no_consumer_for_type");
        c(inner);
    }

    void fire_data(const wire::frame_header &hdr, std::span<const std::byte> inner)
    {
        if(!m_unidirectional)
            return drop("plexus: router no_consumer_for_type");
        m_unidirectional(hdr, inner);
    }

    void drop(std::string_view message)
    {
        m_logger.warn(message);
    }

    log::logger &m_logger;
    data_consumer m_unidirectional;
    consumer m_subscribe;
    consumer m_fetch_latched;
    consumer m_unsubscribe;
    consumer m_subscribe_response;
    consumer m_rpc_request;
    consumer m_rpc_response;
    consumer m_handshake_req;
    consumer m_handshake_resp;
    consumer m_heartbeat;
    consumer m_declare;
};

}

#endif
