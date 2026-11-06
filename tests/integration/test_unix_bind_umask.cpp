// This executable interposes umask and counts every call; the header-only listener code compiled into it
// resolves umask to the counter, so a start that saves and restores the process umask is visible here.

#include "plexus/asio/unix_listener.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <dlfcn.h>

#include <sys/un.h>
#include <sys/stat.h>

#include <set>
#include <atomic>
#include <string>
#include <thread>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <system_error>

namespace pasio = plexus::asio;
namespace pio   = plexus::io;

namespace {

std::atomic<std::int32_t> g_umask_calls{0};

}

extern "C" ::mode_t umask(::mode_t mask) noexcept(noexcept(::umask(0)))
{
    using umask_fn         = ::mode_t (*)(::mode_t);
    static const auto next = reinterpret_cast<umask_fn>(::dlsym(RTLD_NEXT, "umask"));
    ++g_umask_calls;
    return next(mask);
}

namespace {

constexpr ::mode_t k_widened = 0770;

struct bind_dir
{
    std::string dir;
    std::string path;

    bind_dir()
    {
        char tmpl[]      = "/tmp/pxbind-XXXXXX";
        const char *made = ::mkdtemp(tmpl);
        dir              = made ? made : "";
        path             = dir + "/s";
    }

    ~bind_dir()
    {
        std::error_code ec;
        (void)std::filesystem::remove_all(dir, ec);
    }
};

::mode_t mode_of(const std::string &path)
{
    struct ::stat st{};
    if(::stat(path.c_str(), &st) != 0)
        return 0;
    return st.st_mode & 07777;
}

std::vector<std::string> entries_of(const std::string &dir)
{
    std::vector<std::string> names;
    std::error_code ec;
    for(const auto &entry : std::filesystem::directory_iterator(dir, ec))
        names.push_back(entry.path().filename().string());
    return names;
}

std::int32_t umask_calls_across_start(::mode_t mode)
{
    bind_dir t;
    ::asio::io_context io;
    pasio::unix_listener listener{io, {}, mode};
    const std::int32_t before = g_umask_calls.load();
    listener.start(pio::endpoint{"unix", t.path});
    REQUIRE(mode_of(t.path) != 0);
    listener.stop();
    return g_umask_calls.load() - before;
}

void note_mode(std::set<::mode_t> &seen, const std::string &path)
{
    if(const ::mode_t m = mode_of(path); m != 0)
        seen.insert(m & 0777);
}

std::set<::mode_t> modes_seen_while_starting(::mode_t mode)
{
    bind_dir t;
    std::set<::mode_t> seen;
    std::atomic<bool> watching{false};
    std::atomic<bool> started{false};
    std::thread watcher(
            [&]
            {
                watching = true;
                while(!started)
                    note_mode(seen, t.path);
                note_mode(seen, t.path);
            });
    while(!watching)
        std::this_thread::yield();
    ::asio::io_context io;
    pasio::unix_listener listener{io, {}, mode};
    listener.start(pio::endpoint{"unix", t.path});
    started = true;
    watcher.join();
    return seen;
}

}

TEST_CASE("unix_permissions: starting a unix listener never touches the process umask", "[integration][unix][permissions]")
{
    CHECK(umask_calls_across_start(pasio::unix_listener::default_socket_mode) == 0);
    CHECK(umask_calls_across_start(k_widened) == 0);
}

TEST_CASE("unix_permissions: the socket appears at its path with exactly its configured mode", "[integration][unix][permissions]")
{
    CHECK(modes_seen_while_starting(pasio::unix_listener::default_socket_mode) == std::set<::mode_t>{0700});
    CHECK(modes_seen_while_starting(k_widened) == std::set<::mode_t>{k_widened});
}

TEST_CASE("unix_permissions: binding leaves no private directory beside the socket", "[integration][unix][permissions]")
{
    bind_dir t;
    ::asio::io_context io;
    pasio::unix_listener listener{io};
    listener.start(pio::endpoint{"unix", t.path});
    CHECK(entries_of(t.dir) == std::vector<std::string>{"s"});
    listener.stop();
    CHECK(entries_of(t.dir).empty());
}

TEST_CASE("unix_permissions: a target too long for the private bind path is refused and leaves nothing behind", "[integration][unix][permissions]")
{
    // The target fills sun_path exactly, so any private directory beside it pushes the bind path past the limit.
    constexpr std::size_t sun_size = sizeof(::sockaddr_un{}.sun_path);
    bind_dir t;
    const std::string dir = t.dir + "/" + std::string(sun_size - 3 - t.dir.size() - 1, 'd');
    REQUIRE(std::filesystem::create_directory(dir));
    const std::string target = dir + "/s";
    REQUIRE(target.size() == sun_size - 1);

    ::asio::io_context io;
    std::int32_t errors = 0;
    pasio::unix_listener listener{io};
    listener.on_error([&](pio::io_error) { ++errors; });
    listener.start(pio::endpoint{"unix", target});
    CHECK(errors == 1);
    CHECK(entries_of(dir).empty());
}
