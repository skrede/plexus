#ifndef HPP_GUARD_TESTS_INTEGRATION_OUTOFBOX_MESSAGE_CEILING_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_OUTOFBOX_MESSAGE_CEILING_COMMON_H

// The out-of-box message-ceiling guard: an 8 MiB message — the SHIPPED default per-message
// ceiling (io::global_default_max_message_bytes) — round-trips byte-identically over every
// reliable transport at FULL SHIPPED DEFAULTS. No write_queue_bytes, backpressure_bytes,
// global_default, or reassembly-budget bump: every transport is constructed with its
// defaults (the default 16 MiB reassembly budget alone holds an 8 MiB message). This is the
// regression guard for the operator's promise — the local back-pressure / write-queue caps
// can never again silently strangle a message within the shipped ceiling. The negotiated
// ceiling is the sole size authority; the back-pressure cap is decoupled from it.
//
// tcp / unix / tls carry a framed wire message over the stream reassembler; udpr carries the
// raw payload over the reliable ARQ + datagram reassembler. (DTLS best-effort has the
// documented ~3 MiB host delivery cap and is excluded from this reliable 8 MiB round-trip.)
// Each leg loops in-body; the ctest invocation is re-run across process runs (a transport
// claim is never made from one run). A position ramp catches a reorder/corruption.

#include "plexus/asio/asio_transport.h"
#include "plexus/asio/unix_transport.h"
#include "plexus/asio/udp_transport.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/fragmentation.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/frame_codec.h"
#include "plexus/wire/close_cause.h"

#ifdef PLEXUS_HAVE_TLS_OUTOFBOX
    #include "plexus/tls/tls_channel.h"
    #include "plexus/tls/tls_transport.h"
    #include "plexus/tls/tls_credential.h"
    #include "plexus/tls/spki_fingerprint.h"

    #include "plexus/io/security/verify_policy.h"

    #include <openssl/evp.h>
    #include <openssl/pem.h>
    #include <openssl/x509.h>
#endif

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>

#include <unistd.h>

#include <span>
#include <array>
#include <chrono>
#include <memory>
#include <string>
#include <vector>
#include <cstdio>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>
#include <algorithm>
#include <filesystem>

namespace pasio = plexus::asio;
namespace pio   = plexus::io;
namespace wire  = plexus::wire;

namespace outofbox_ceiling_fixture {

using ms = std::chrono::milliseconds;

// The SHIPPED default ceiling — the size this test proves round-trips at full defaults.
constexpr std::size_t k_shipped_ceiling = pio::global_default_max_message_bytes; // 8 MiB

// A deterministic position-dependent payload, regenerated to verify the round-trip is
// byte-identical (a size match alone would miss a reorder or a corrupt fragment).
inline std::vector<std::byte> ramp_payload(std::size_t n)
{
    std::vector<std::byte> out(n);
    for(std::size_t i = 0; i < n; ++i)
        out[i] = static_cast<std::byte>((i * 31u + (i >> 8)) & 0xFFu);
    return out;
}

inline bool equal_bytes(std::span<const std::byte> a, std::span<const std::byte> b)
{
    return a.size() == b.size() && std::equal(a.begin(), a.end(), b.begin());
}

template<typename Pred>
inline void pump_until(::asio::io_context &io, Pred pred, ms timeout = ms{20000})
{
    auto bound = std::chrono::steady_clock::now() + timeout;
    while(!pred() && std::chrono::steady_clock::now() < bound)
    {
        io.poll();
        if(io.stopped())
            io.restart();
    }
}

// A stream frame whose payload is the ceiling-sized body: tcp/unix/tls round-trip the WHOLE
// framed message through the reassembler, so the body sits under a short frame header.
inline std::vector<std::byte> ceiling_frame(const std::vector<std::byte> &body)
{
    wire::frame_header hdr{};
    hdr.type        = wire::msg_type::unidirectional;
    hdr.payload_len = body.size();
    return wire::encode_frame(hdr, std::span<const std::byte>{body});
}

}

#endif
