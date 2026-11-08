// The host half of the on-device lwIP example, driven entirely through the public node facade: a
// node over the host asio TCP transport LISTENS, the ESP32 dials in over Wi-Fi, the host subscribes
// to the telemetry topic, and the executor runs until one real message arrives. It proves the SAME
// node surface the device loop drives also drives the host: a public listen verb, a subscriber, and
// the executor run loop, with no below-facade session construction.
//
// The device is the dialer here (it joins the AP, gets a DHCP lease, then dials the fixed host
// endpoint), so this side binds and accepts: node.listen, not node.dial. The board auto-reset pulse
// is NOT here — the host server holds no serial fd to the board; the runner drives that on the flash
// port. The accept deadline is generous because the device must associate + lease before it dials.
//
// PLEXUS_HOST_HOLD=<s> re-arms the deadline to that many seconds after the first message, and
// PLEXUS_HOST_DROP_AFTER=<n> exits after n messages, dropping the session; such a run passes only once n messages
// arrived, so a deadline that fires first fails it. With neither set the run ends, and passes, at the first message.

#include "plexus/node.h"
#include "plexus/subscriber.h"
#include "plexus/node_options.h"

#include "plexus/discovery/static_discovery.h"

#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_transport.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/reconnect_config.h"

#include <asio/io_context.hpp>
#include <asio/steady_timer.hpp>

#include <span>
#include <string>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstddef>
#include <charconv>
#include <iostream>
#include <optional>
#include <string_view>
#include <system_error>

namespace pasio = plexus::asio;

namespace {

using namespace std::chrono_literals;

constexpr const char *k_topic        = "telemetry";
constexpr const char *k_default_port = "7447";

// The device must join the AP, take a DHCP lease, then dial — seconds, not the serial gate's
// sub-second boot. Association can retry on a missed first attempt (each cycle adds several
// seconds), so the deadline is generous enough that a slow join never false-fails the gate.
constexpr std::chrono::seconds k_timeout{60};

std::string listen_address(int argc, char **argv)
{
    std::string_view port = k_default_port;
    if(argc > 1)
        port = argv[1];
    else if(const char *env = std::getenv("PLEXUS_HOST_PORT"))
        port = env;
    return "0.0.0.0:" + std::string{port};
}

// An unset, zero or malformed value reads as absent, which keeps the stop-at-first-message default.
std::optional<std::uint32_t> env_count(const char *name)
{
    const char *env = std::getenv(name);
    if(env == nullptr)
        return std::nullopt;
    const std::string_view text{env};
    std::uint32_t value  = 0;
    const auto [end, ec] = std::from_chars(text.data(), text.data() + text.size(), value);
    if(ec != std::errc{} || end != text.data() + text.size() || value == 0)
        return std::nullopt;
    return value;
}

void stop_after(::asio::steady_timer &deadline, ::asio::io_context &io, std::chrono::seconds after)
{
    deadline.expires_after(after);
    deadline.async_wait(
        [&io](std::error_code ec)
        {
            if(!ec)
                io.stop();
        });
}

struct message_tally
{
    ::asio::io_context &io;
    std::uint32_t count{0};
    ::asio::steady_timer &deadline;
    std::optional<std::uint8_t> last{};
    std::optional<std::uint32_t> hold{};
    std::optional<std::uint32_t> drop_after{};

    void on_message(std::span<const std::byte> bytes)
    {
        if(!bytes.empty())
            last = static_cast<std::uint8_t>(bytes[0]);
        ++count;
        if(drop_after.has_value() && count >= *drop_after)
            io.stop();
        else if(hold.has_value() && count == 1)
            stop_after(deadline, io, std::chrono::seconds{*hold});
        else if(!hold.has_value() && !drop_after.has_value())
            io.stop();
    }
};

int report(const message_tally &tally)
{
    const std::uint32_t needed = tally.drop_after.value_or(1);
    if(tally.last.has_value() && tally.count >= needed)
    {
        std::cout << "GATE_PASS value=" << static_cast<std::uint32_t>(*tally.last) << " count=" << tally.count << '\n';
        return 0;
    }
    std::cout << "GATE_FAIL count=" << tally.count << " needed=" << needed << '\n';
    return 1;
}

}

int main(int argc, char **argv)
{
    ::asio::io_context io;
    pasio::asio_transport transport{io};

    // The device dials a fixed host endpoint, so the host needs no IP discovery: the table is empty.
    plexus::discovery::static_discovery disc{{}};

    plexus::node_options opts;
    opts.name      = "lwip-gate-host";
    opts.reconnect = plexus::io::reconnect_config{200ms, 5s, std::nullopt, std::nullopt};

    plexus::node<pasio::asio_policy, pasio::asio_transport> node{io, disc, "lwip-gate-host", transport, opts};

    ::asio::steady_timer deadline{io};
    message_tally tally{.io = io, .deadline = deadline, .hold = env_count("PLEXUS_HOST_HOLD"), .drop_after = env_count("PLEXUS_HOST_DROP_AFTER")};
    plexus::subscriber<> telemetry{node, k_topic, [&tally](std::span<const std::byte> bytes, const plexus::io::message_info &)
                                   { tally.on_message(bytes); }};

    node.listen({"tcp", listen_address(argc, argv)});
    stop_after(deadline, io, k_timeout);

    io.run();
    return report(tally);
}
