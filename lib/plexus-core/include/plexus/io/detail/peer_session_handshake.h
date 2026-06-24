#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_HANDSHAKE_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_PEER_SESSION_HANDSHAKE_H

#include "plexus/io/handshake_protocol.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/handshake.h"
#include "plexus/wire/heartbeat.h"
#include "plexus/wire/frame_codec.h"

namespace plexus::io::detail {

static wire::handshake_status status_for(handshake_outcome outcome) noexcept
{
    switch(outcome)
    {
        case handshake_outcome::reject_version:
            return wire::handshake_status::version_incompatible;
        case handshake_outcome::reject_identity:
            return wire::handshake_status::identity_conflict;
        case handshake_outcome::reject_unauthorized:
            return wire::handshake_status::unauthorized;
        default:
            return wire::handshake_status::accepted;
    }
}

// A secured node advertises its AEAD posture in cipher_offer/chosen so the peer's posture gate
// sees a secured-vs-secured pair; a plain node leaves them zero.
template<typename Session>
wire::handshake_request self_request(const Session &s) noexcept
{
    const std::uint8_t offer = s.m_negotiator.cipher_offer();
    return {.id                       = s.m_fsm_cfg.self_id,
            .version_major            = s.m_fsm_cfg.version_major,
            .version_minor            = s.m_fsm_cfg.version_minor,
            .compatible_version_major = s.m_fsm_cfg.compatible_version_major,
            .compatible_version_minor = s.m_fsm_cfg.compatible_version_minor,
            .protocol_version         = wire::k_protocol_version,
            .fingerprint              = s.m_fsm_cfg.local_fingerprint.value,
            .key_id                   = s.m_negotiator.key_id(),
            .own_nonce                = s.m_negotiator.own_nonce(),
            .cipher_offer             = offer,
            .chosen_cipher            = offer,
            .proof                    = {}};
}

// Handshake control always carries session_id 0 — it is what mints the epoch.
template<typename Session>
void send_control(Session &s, wire::msg_type type)
{
    wire::frame_header fhdr{.type = type, .flags = 0, .session_id = 0, .timestamp_ns = wire::now_timestamp_ns(), .payload_len = s.m_payload_scratch.size()};
    wire::encode_frame_into(s.m_frame_scratch, fhdr, s.m_payload_scratch);
    s.m_channel.send(s.m_frame_scratch);
}

template<typename Session>
void send_handshake_request(Session &s)
{
    wire::encode_handshake_request_into(s.m_payload_scratch, self_request(s));
    send_control(s, wire::msg_type::handshake_req);
}

// The response is the one leg the local node proves on: a request carries no proof (a 1-RTT
// PSK dialer cannot prove before seeing the responder's nonce). The proof MACs the dialer's
// facts-view so the dialer, recomputing under the shared PSK with its mirror view, matches.
template<typename Session>
void send_handshake_response(Session &s, handshake_outcome outcome)
{
    const auto r = self_request(s);
    wire::handshake_response resp{.id                       = r.id,
                                  .version_major            = r.version_major,
                                  .version_minor            = r.version_minor,
                                  .compatible_version_major = r.compatible_version_major,
                                  .compatible_version_minor = r.compatible_version_minor,
                                  .protocol_version         = r.protocol_version,
                                  .fingerprint              = r.fingerprint,
                                  .key_id                   = r.key_id,
                                  .own_nonce                = r.own_nonce,
                                  .cipher_offer             = r.cipher_offer,
                                  .chosen_cipher            = r.chosen_cipher,
                                  .proof                    = s.m_negotiator.response_proof(),
                                  .status                   = status_for(outcome)};
    wire::encode_handshake_response_into(s.m_payload_scratch, resp);
    send_control(s, wire::msg_type::handshake_resp);
}

}

#endif
