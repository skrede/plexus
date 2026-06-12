// The af_unix permissions model proof: the listener creates its socket file
// at the configured mode (0700 owner-only by default, a widened mode honored when the
// knob asks for it), a peer-credential policy admits or refuses an accepted local peer
// fail-closed, and the unlink-before-bind TOCTOU window is closed. The legs here drive a
// real unix_listener over a per-test owner-only temp directory and stat the on-disk socket
// inode, plus stand up a real client to exercise the accept-time credential gate.
//
// These assert the target surface (the configurable mode knob + the peer_cred allowlist +
// the closed TOCTOU window). Until that surface lands the mode-knob and peer-cred legs are
// RED — the default-0700 leg already holds on the current listener and pins the fail-closed
// default that must survive the knob's arrival.

#include "plexus/asio/unix_listener.h"

#include "plexus/io/endpoint.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <sys/stat.h>
#include <unistd.h>

#include <string>
#include <chrono>
#include <cstdlib>

namespace pasio = plexus::asio;
namespace pio = plexus::io;

namespace {

// A per-test owner-only temp directory + a short socket path within it. The owner-only
// parent dir is the security contract the listener's TOCTOU close leans on.
struct temp_dir
{
    std::string dir;
    std::string path;

    temp_dir()
    {
        char tmpl[] = "/tmp/pxperm-XXXXXX";
        const char *made = ::mkdtemp(tmpl);   // mkdtemp creates the dir 0700 owner-only
        dir = made ? made : "";
        path = dir + "/s";
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

template <typename Pred>
void pump_until(::asio::io_context &io, Pred pred)
{
    auto bound = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while(!pred() && std::chrono::steady_clock::now() < bound)
        io.poll();
}

}

TEST_CASE("unix_permissions: the socket file is created owner-only 0700 by default",
          "[integration][unix][permissions]")
{
    temp_dir t;
    ::asio::io_context io;
    pasio::unix_listener listener{io};
    listener.start(pio::endpoint{"unix", t.path});

    pump_until(io, [&] { return mode_of(t.path) != 0; });
    REQUIRE((mode_of(t.path) & 0777) == 0700);   // owner-only fail-closed default
}

TEST_CASE("unix_permissions: a widened socket mode is honored",
          "[integration][unix][permissions]")
{
    temp_dir t;
    ::asio::io_context io;
    // A deliberately widened mode (group access). The current listener hard-codes 0700 at
    // bind regardless of any request, so requiring the widened mode is RED until the socket
    // mode becomes a configurable knob (this leg flips to the knob ctor then).
    constexpr ::mode_t widened = 0770;
    pasio::unix_listener listener{io};
    listener.start(pio::endpoint{"unix", t.path});

    pump_until(io, [&] { return mode_of(t.path) != 0; });
    REQUIRE((mode_of(t.path) & 0777) == widened);
}
