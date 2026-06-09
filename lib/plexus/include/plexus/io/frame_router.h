#ifndef HPP_GUARD_PLEXUS_IO_FRAME_ROUTER_H
#define HPP_GUARD_PLEXUS_IO_FRAME_ROUTER_H

#include "plexus/io/null_logger.h"
#include "plexus/log/logger.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/frame_codec.h"

#include "plexus/detail/compat.h"

#include <span>
#include <cstddef>
#include <utility>
#include <string_view>

namespace plexus::io {

// The receive-side demux that OWNS the frame_header.type switch so the message
// and procedure forwarders stay single-responsibility (the router-owns-demux
// split: a forwarder exposes deliver_* and never switches on frame type). It is
// backend-agnostic — it operates on spans + move_only_function, no Policy
// template. route() takes a COMPLETE frame (header-ON): it strips the
// frame_header and hands each consumer the INNER payload (header-OFF). Because
// both backends deliver header-on full frames to on_data (inproc verbatim, asio
// re-framed after reassembly), route() is uniform across inproc and asio.
//
// An untrusted frame — short, bad-magic, or carrying an unregistered/unknown
// type — is warn-and-DROPPED through the injected logger& (default the silent
// shared null_logger): never thrown, never crashed.
class frame_router
{
public:
    using consumer = plexus::detail::move_only_function<void(std::span<const std::byte>)>;
    // The data-path consumer also receives the decoded frame_header, because the
    // per-message metadata the subscriber sees (source_timestamp, the session epoch)
    // lives in the header that route() otherwise strips before deliver. Handing it
    // alongside the inner payload is what keeps the forwarder header-aware ONLY on the
    // one path that needs it, without re-decoding the header downstream.
    using data_consumer =
        plexus::detail::move_only_function<void(const wire::frame_header &, std::span<const std::byte>)>;

    explicit frame_router(log::logger &logger = shared_null_logger()) noexcept
        : m_logger(logger)
    {
    }

    void on_unidirectional(data_consumer c) { m_unidirectional = std::move(c); }
    void on_subscribe(consumer c) { m_subscribe = std::move(c); }
    void on_unsubscribe(consumer c) { m_unsubscribe = std::move(c); }
    void on_subscribe_response(consumer c) { m_subscribe_response = std::move(c); }
    void on_rpc_request(consumer c) { m_rpc_request = std::move(c); }
    void on_rpc_response(consumer c) { m_rpc_response = std::move(c); }
    void on_handshake_req(consumer c) { m_handshake_req = std::move(c); }
    void on_handshake_resp(consumer c) { m_handshake_resp = std::move(c); }

    // Demux one complete (header-on) frame: decode the header, switch on its
    // type, and hand the inner payload to the registered consumer. A short/
    // bad-magic frame or an unknown/unregistered type is warn-and-dropped.
    void route(std::span<const std::byte> frame)
    {
        auto hdr = wire::decode_header(frame);
        if(!hdr)
            return drop("plexus: router frame_decode_failed");

        auto inner = frame.subspan(wire::header_size);
        dispatch(*hdr, inner);
    }

private:
    void dispatch(const wire::frame_header &hdr, std::span<const std::byte> inner)
    {
        switch(hdr.type)
        {
            case wire::msg_type::unidirectional:     return fire_data(hdr, inner);
            case wire::msg_type::subscribe:          return fire(m_subscribe, inner);
            case wire::msg_type::unsubscribe:        return fire(m_unsubscribe, inner);
            case wire::msg_type::subscribe_response: return fire(m_subscribe_response, inner);
            case wire::msg_type::rpc_request:        return fire(m_rpc_request, inner);
            case wire::msg_type::rpc_response:       return fire(m_rpc_response, inner);
            case wire::msg_type::handshake_req:      return fire(m_handshake_req, inner);
            case wire::msg_type::handshake_resp:     return fire(m_handshake_resp, inner);
            default:                                 return drop("plexus: router unknown_frame_type");
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

    void drop(std::string_view message) { m_logger.warn(message); }

    log::logger &m_logger;
    data_consumer m_unidirectional;
    consumer m_subscribe;
    consumer m_unsubscribe;
    consumer m_subscribe_response;
    consumer m_rpc_request;
    consumer m_rpc_response;
    consumer m_handshake_req;
    consumer m_handshake_resp;
};

}

#endif
