#include "plexus/asio/unix_channel.h"
#include "plexus/asio/unix_listener.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"

#include "plexus/testing/pump.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>
#include <asio/local/stream_protocol.hpp>

#include <sys/un.h>

#include <chrono>
#include <memory>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <utility>
#include <filesystem>
#include <system_error>

namespace pasio = plexus::asio;
namespace pio   = plexus::io;

namespace {

constexpr std::size_t k_sun_size         = sizeof(::sockaddr_un{}.sun_path);
constexpr std::size_t k_private_overhead = sizeof("/.XXXXXX/s") - 1;
constexpr std::chrono::seconds k_pump_bound{5};

// A parent directory whose path is exactly `length` bytes, so the private bind path beside a target in it is sized
// to the byte against sun_path.
struct sized_parent
{
    std::string root;
    std::string dir;

    explicit sized_parent(std::size_t length)
    {
        char tmpl[]      = "/tmp/pxlimit-XXXXXX";
        const char *made = ::mkdtemp(tmpl);
        root             = made ? made : "";
        dir              = root + "/" + std::string(length - root.size() - 1, 'd');
        std::filesystem::create_directory(dir);
    }

    ~sized_parent()
    {
        std::error_code ec;
        (void)std::filesystem::remove_all(root, ec);
    }
};

std::vector<std::string> entries_of(const std::string &dir)
{
    std::vector<std::string> names;
    std::error_code ec;
    for(const auto &entry : std::filesystem::directory_iterator(dir, ec))
        names.push_back(entry.path().filename().string());
    return names;
}

}

TEST_CASE("unix_permissions: a target whose shortest private bind path fits sun_path binds and accepts", "[integration][unix][permissions]")
{
    sized_parent parent{k_sun_size - 1 - k_private_overhead};
    REQUIRE(std::filesystem::is_directory(parent.dir));
    REQUIRE(parent.dir.size() + k_private_overhead == k_sun_size - 1);
    const std::string target = parent.dir + "/s";

    ::asio::io_context io;
    std::int32_t errors   = 0;
    std::int32_t accepted = 0;
    std::unique_ptr<pasio::unix_channel> admitted;
    pasio::unix_listener listener{io};
    listener.on_error([&](pio::io_error) { ++errors; });
    listener.on_accepted(
            [&](std::unique_ptr<pasio::unix_channel> ch)
            {
                ++accepted;
                admitted = std::move(ch);
            });
    listener.start(pio::endpoint{"unix", target});
    REQUIRE(errors == 0);

    ::asio::local::stream_protocol::socket client{io};
    client.connect(::asio::local::stream_protocol::endpoint(target));
    REQUIRE(plexus::testing::pump_until(io, [&] { return accepted > 0; }, k_pump_bound));
    CHECK(entries_of(parent.dir) == std::vector<std::string>{"s"});
}

TEST_CASE("unix_permissions: a target whose shortest private bind path overflows sun_path is refused and leaves nothing behind", "[integration][unix][permissions]")
{
    sized_parent parent{k_sun_size - k_private_overhead};
    REQUIRE(std::filesystem::is_directory(parent.dir));
    REQUIRE(parent.dir.size() + k_private_overhead == k_sun_size);
    const std::string target = parent.dir + "/s";
    REQUIRE(target.size() < k_sun_size);

    ::asio::io_context io;
    std::int32_t errors = 0;
    pasio::unix_listener listener{io};
    listener.on_error([&](pio::io_error) { ++errors; });
    listener.start(pio::endpoint{"unix", target});
    CHECK(errors == 1);
    CHECK(entries_of(parent.dir).empty());
}
