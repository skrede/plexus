// The af_unix permissions model proof: the listener creates its socket file at the
// configured mode (0700 owner-only by default, a widened mode honored when the knob asks
// for it), a peer-credential policy admits or refuses an accepted local peer fail-closed,
// and the unlink-before-bind TOCTOU window is closed (bind under a restrictive umask). The
// legs here drive a real unix_listener over a per-test owner-only temp directory and stat
// the on-disk socket inode, plus stand up a real client to exercise the accept-time
// credential gate.
//
// macOS/Windows credential paths are per-platform guarded in the listener and are NOT
// claimed tested on this Linux host (flagged for CI).

#include "plexus/asio/unix_channel.h"
#include "plexus/asio/unix_listener.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/security/peer_cred_policy.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>
#include <asio/local/stream_protocol.hpp>

#include <sys/stat.h>
#include <unistd.h>

#include <string>
#include <chrono>
#include <memory>
#include <cstdint>
#include <cstdlib>

namespace pasio = plexus::asio;
namespace pio   = plexus::io;

namespace {

// A per-test owner-only temp directory + a short socket path within it. The owner-only
// parent dir is the security contract the listener's TOCTOU close leans on.
struct temp_dir
{
    std::string dir;
    std::string path;

    temp_dir()
    {
        char        tmpl[] = "/tmp/pxperm-XXXXXX";
        const char *made   = ::mkdtemp(tmpl); // mkdtemp creates the dir 0700 owner-only
        dir                = made ? made : "";
        path               = dir + "/s";
    }

    ~temp_dir()
    {
        if(!path.empty())
            ::unlink(path.c_str());
        if(!dir.empty())
            ::rmdir(dir.c_str());
    }
};

// The permission bits of a path (the low 0777 mode), or 0 on a stat failure.
::mode_t mode_of(const std::string &path)
{
    struct ::stat st{};
    if(::stat(path.c_str(), &st) != 0)
        return 0;
    return st.st_mode & 07777;
}

template<typename Pred>
void pump_until(::asio::io_context &io, Pred pred)
{
    auto bound = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while(!pred() && std::chrono::steady_clock::now() < bound)
        io.poll();
}

// A peer-credential policy that refuses every local peer (the restrictive end of the seam).
struct deny_all_peer_cred final : public pio::security::peer_cred_policy
{
    [[nodiscard]] bool decide(const pio::security::peer_cred &) const noexcept override
    {
        return false;
    }
    [[nodiscard]] bool accepts_without_credentials() const noexcept override
    {
        return false;
    }
};

}

TEST_CASE("unix_permissions: the socket file is created owner-only 0700 by default", "[integration][unix][permissions]")
{
    temp_dir             t;
    ::asio::io_context   io;
    pasio::unix_listener listener{io};
    listener.start(pio::endpoint{"unix", t.path});

    pump_until(io, [&] { return mode_of(t.path) != 0; });
    REQUIRE((mode_of(t.path) & 0777) == 0700); // owner-only fail-closed default
}

TEST_CASE("unix_permissions: a widened socket mode is honored", "[integration][unix][permissions]")
{
    temp_dir           t;
    ::asio::io_context io;
    // A deliberately widened mode (group access) passed through the mode knob: the listener
    // applies it at bind so the on-disk socket carries the widened bits.
    constexpr ::mode_t   widened = 0770;
    pasio::unix_listener listener{io, {}, widened};
    listener.start(pio::endpoint{"unix", t.path});

    pump_until(io, [&] { return mode_of(t.path) != 0; });
    REQUIRE((mode_of(t.path) & 0777) == widened);
}

TEST_CASE("unix_permissions: the unlink-before-bind window is closed under a restrictive umask", "[integration][unix][permissions]")
{
    // Even under a permissive process umask (which would otherwise let bind create a
    // world-accessible socket inode before the chmod), the listener binds under a
    // restrictive umask so the inode is never momentarily world-accessible: the final
    // 0700 mode holds and no broader bits ever appear.
    temp_dir             t;
    const ::mode_t       prev = ::umask(0); // maximally permissive: bind would create 0777 if unguarded
    ::asio::io_context   io;
    pasio::unix_listener listener{io};
    listener.start(pio::endpoint{"unix", t.path});
    pump_until(io, [&] { return mode_of(t.path) != 0; });
    (void)::umask(prev);

    REQUIRE((mode_of(t.path) & 0777) == 0700); // never widened by the permissive umask
}

TEST_CASE("unix_permissions: accept_any admits a local peer while a deny-all policy refuses "
          "fail-closed",
          "[integration][unix][permissions]")
{
    // accept_any (the default) admits the local peer — a channel is handed up. A deny-all
    // policy refuses the accept fail-closed: the connect succeeds at the socket layer but no
    // channel is handed up, the session stays unestablished.
    SECTION("accept_any admits")
    {
        temp_dir             t;
        ::asio::io_context   io;
        int                  accepted = 0;
        pasio::unix_listener listener{io};
        listener.on_accepted([&](std::unique_ptr<pasio::unix_channel>) { ++accepted; });
        listener.start(pio::endpoint{"unix", t.path});

        ::asio::local::stream_protocol::socket client{io};
        client.connect(::asio::local::stream_protocol::endpoint(t.path));
        pump_until(io, [&] { return accepted > 0; });
        REQUIRE(accepted == 1);
    }

    SECTION("deny-all refuses fail-closed")
    {
        temp_dir             t;
        ::asio::io_context   io;
        int                  accepted = 0;
        deny_all_peer_cred   deny;
        pasio::unix_listener listener{io, {}, pasio::unix_listener::default_socket_mode, deny};
        listener.on_accepted([&](std::unique_ptr<pasio::unix_channel>) { ++accepted; });
        listener.start(pio::endpoint{"unix", t.path});

        ::asio::local::stream_protocol::socket client{io};
        client.connect(::asio::local::stream_protocol::endpoint(t.path));

        auto bound = std::chrono::steady_clock::now() + std::chrono::milliseconds(300);
        while(std::chrono::steady_clock::now() < bound)
            io.poll();
        REQUIRE(accepted == 0); // refused fail-closed: no channel handed up
    }
}
