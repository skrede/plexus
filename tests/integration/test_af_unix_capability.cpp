#include "plexus/asio/unix_policy.h"
#include "plexus/asio/unix_channel.h"
#include "plexus/asio/unix_listener.h"
#include "plexus/asio/detail/asio_unix_endpoint_parse.h"

#include "plexus/io/io_error.h"
#include "plexus/io/frame_router.h"
#include "plexus/io/message_forwarder.h"
#include "plexus/io/security/peer_cred_policy.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/data_frame.h"
#include "plexus/wire/frame_codec.h"

#include "plexus/log/logger.h"

#include "plexus/detail/socket_compat.h"

#include "plexus/testing/platform.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>
#include <asio/local/stream_protocol.hpp>

#include <span>
#include <chrono>
#include <memory>
#include <string>
#include <vector>
#include <cstddef>
#include <optional>
#include <filesystem>
#include <string_view>
#include <system_error>

// A hard floor: asio must expose AF_UNIX (Windows carries it at _WIN32_WINNT >= 0x0A00), so a
// toolchain that silently loses the capability fails here rather than at an obscure link error.
#if !defined(ASIO_HAS_LOCAL_SOCKETS)
    #error "ASIO_HAS_LOCAL_SOCKETS must be defined - AF_UNIX is required (expected at _WIN32_WINNT >= 0x0A00)."
#endif

namespace pasio = plexus::asio;
namespace pio   = plexus::io;
namespace wire  = plexus::wire;

namespace {

std::vector<std::byte> bytes_of(std::string_view s)
{
    std::vector<std::byte> v(s.size());
    for(std::size_t i = 0; i < s.size(); ++i)
        v[i] = static_cast<std::byte>(static_cast<unsigned char>(s[i]));
    return v;
}

struct temp_sock
{
    std::string dir;
    std::string path;

    temp_sock()
    {
        dir  = plexus::testing::make_temp_dir("pxu-").string();
        path = dir + "/s";
    }

    ~temp_sock()
    {
        if(!path.empty())
            plexus::testing::remove_socket_path(path);
        if(!dir.empty())
        {
            std::error_code ec;
            std::filesystem::remove(dir, ec);
        }
    }
};

// accepts_without_credentials()==false forces a Windows AF_UNIX listener (no peer creds) to refuse
// at listen (fail-closed) rather than admit an unidentifiable peer.
class deny_uncredentialed_policy final : public pio::security::peer_cred_policy
{
public:
    bool decide(const pio::security::peer_cred &) const noexcept override
    {
        return false;
    }
    bool accepts_without_credentials() const noexcept override
    {
        return false;
    }
};

// One publish -> receive round-trip over a real AF_UNIX stream socket, through the same frame_router
// receive contract the live TCP path uses; fresh listener+client+context+path per call.
std::optional<std::vector<std::byte>> one_roundtrip(std::span<const std::byte> payload, std::string_view fqn)
{
    temp_sock sock;
    ::asio::io_context io;

    pasio::unix_listener listener(io);
    std::unique_ptr<pasio::unix_channel> server_channel;
    listener.on_accepted([&](std::unique_ptr<pasio::unix_channel> ch) { server_channel = std::move(ch); });
    listener.start({"unix", sock.path});

    pasio::unix_channel client(io);
    std::optional<std::vector<std::byte>> received;
    plexus::log::null_logger router_sink;
    pio::frame_router router{router_sink};
    router.on_unidirectional(
            [&](const wire::frame_header &, std::span<const std::byte> inner)
            {
                auto decoded = wire::decode_unidirectional(inner);
                if(decoded)
                    received = std::vector<std::byte>(decoded->data.begin(), decoded->data.end());
            });
    client.on_data([&](std::span<const std::byte> frame) { router.route(frame); });

    ::asio::local::stream_protocol::endpoint server_ep(sock.path);
    std::error_code cec;
    client.socket().connect(server_ep, cec);
    if(cec)
        return std::nullopt;
    client.start_read();

    auto accept_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while(!server_channel && std::chrono::steady_clock::now() < accept_deadline)
        io.poll_one();
    if(!server_channel)
        return std::nullopt;

    plexus::log::null_logger sink;
    pio::message_forwarder<pasio::unix_policy> fwd{sink};
    pio::message_forwarder<pasio::unix_policy>::peer sub{*server_channel, "client-node"};
    fwd.attach(sub, fqn);
    fwd.publish(fqn, payload);

    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while(!received && std::chrono::steady_clock::now() < deadline)
        io.poll();

    return received;
}

}

TEST_CASE("a unix-domain (AF_UNIX) stream socket round-trips a published message end to end, looped", "[integration][unix][afunix]")
{
    const auto payload    = bytes_of("plexus-af-unix-capability-payload");
    const std::string fqn = "demo._plexus._unix.local.";

    constexpr int k_iterations = 50;
    int delivered              = 0;
    for(int i = 0; i < k_iterations; ++i)
    {
        auto got = one_roundtrip(payload, fqn);
        REQUIRE(got.has_value());
        REQUIRE(*got == payload);
        ++delivered;
    }
    REQUIRE(delivered == k_iterations);
}

TEST_CASE("a unix-domain listener fails closed under a no-credential peer policy when peer credentials are unavailable", "[integration][unix][afunix][security]")
{
    temp_sock sock;
    ::asio::io_context io;

    deny_uncredentialed_policy policy; // declared before the listener so it outlives the borrowed reference
    pasio::unix_listener listener(io, {}, pasio::unix_listener::default_socket_mode, policy);
    std::optional<pio::io_error> reported;
    listener.on_error([&](pio::io_error e) { reported = e; });

    listener.start({"unix", sock.path});
    for(int i = 0; i < 64; ++i)
        io.poll_one();

    // Windows AF_UNIX carries no peer creds, so a non-accept-any policy is refused at listen
    // (operation_not_supported, folded to io_error::unknown). Elsewhere creds are judged at accept.
    if constexpr(!plexus::detail::peer_cred_supported)
        REQUIRE(reported.has_value());
    else
        REQUIRE_FALSE(reported.has_value());
}

TEST_CASE("parse_unix fails closed on empty and oversized unix socket paths", "[integration][unix][afunix][security]")
{
    std::error_code ec;
    (void)pasio::detail::parse_unix("", ec);
    REQUIRE(ec);
    ec.clear();
    const std::string oversized(4096, 'a');
    (void)pasio::detail::parse_unix(oversized, ec);
    REQUIRE(ec);

    ec.clear();
    (void)pasio::detail::parse_unix("pxs", ec);
    REQUIRE_FALSE(ec);
}
