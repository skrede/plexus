#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"

#include <catch2/catch_test_macros.hpp>

#include <memory>
#include <string>
#include <vector>
#include <cstdint>
#include <utility>

using plexus::io::endpoint;
using plexus::io::io_error;
using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_channel;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_transport;
using channel_list = std::vector<std::unique_ptr<inproc_channel<>>>;
using error_list   = std::vector<io_error>;

namespace {

const endpoint k_one{"inproc", "one"};

struct dialer
{
    dialer(inproc_executor<> &ex, inproc_bus<> &bus)
            : refused()
            , dialed()
            , transport(ex, bus)
    {
        transport.on_dialed([this](std::unique_ptr<inproc_channel<>> ch, const endpoint &) { dialed.push_back(std::move(ch)); });
        transport.on_dial_failed([this](const endpoint &, io_error e) { refused.push_back(e); });
    }

    error_list refused;
    channel_list dialed;
    inproc_transport<> transport;
};

// The listener callback's only capture is a shared_ptr, small enough to sit inline in the callback's storage, and it
// is the sole owner of this state, so a callback destroyed or relocated mid-call loses it.
struct accept_state
{
    inproc_bus<> &bus;
    channel_list &accepted;
    std::uint64_t &own_id;
};

}

TEST_CASE("inproc accept: a listener callback that grows the listener table keeps its own captures", "[inproc]")
{
    inproc_bus<> bus;
    inproc_executor<> ex(bus);
    dialer d(ex, bus);
    channel_list accepted;
    std::uint64_t own_id  = 0;
    auto grow_then_accept = [state = std::make_shared<accept_state>(accept_state{bus, accepted, own_id})](std::unique_ptr<inproc_channel<>> ch)
    {
        for(std::uint32_t i = 0; i < 16; ++i)
            state->bus.register_listener(endpoint{"inproc", "grown-" + std::to_string(i)}, [](std::unique_ptr<inproc_channel<>>) {});
        state->accepted.push_back(std::move(ch));
    };
    own_id = bus.register_listener(k_one, std::move(grow_then_accept));
    d.transport.dial(k_one);
    ex.drain();
    REQUIRE(accepted.size() == 1);
    REQUIRE(d.dialed.size() == 1);
}

TEST_CASE("inproc accept: a listener callback that deregisters itself keeps its own captures", "[inproc]")
{
    inproc_bus<> bus;
    inproc_executor<> ex(bus);
    dialer d(ex, bus);
    channel_list accepted;
    std::uint64_t own_id   = 0;
    auto leave_then_accept = [state = std::make_shared<accept_state>(accept_state{bus, accepted, own_id})](std::unique_ptr<inproc_channel<>> ch)
    {
        state->bus.deregister_listener(state->own_id);
        state->accepted.push_back(std::move(ch));
    };
    own_id = bus.register_listener(k_one, std::move(leave_then_accept));
    d.transport.dial(k_one);
    ex.drain();
    REQUIRE(accepted.size() == 1);
    REQUIRE(d.dialed.size() == 1);

    d.transport.dial(k_one);
    ex.drain();
    REQUIRE(d.refused == error_list{io_error::connection_refused});
}

TEST_CASE("inproc accept: an accept handler that closes its own transport completes the dial and refuses the next", "[inproc]")
{
    inproc_bus<> bus;
    inproc_executor<> ex(bus);
    dialer d(ex, bus);
    channel_list accepted;
    inproc_transport<> listener(ex, bus);
    listener.on_accepted(
            [&](std::unique_ptr<inproc_channel<>> ch)
            {
                listener.close();
                accepted.push_back(std::move(ch));
            });
    listener.listen(k_one);
    d.transport.dial(k_one);
    ex.drain();
    REQUIRE(accepted.size() == 1);
    REQUIRE(d.dialed.size() == 1);

    d.transport.dial(k_one);
    ex.drain();
    REQUIRE(d.refused == error_list{io_error::connection_refused});
}

TEST_CASE("inproc accept: an accept handler that destroys its listening transport completes the dial and refuses the next", "[inproc]")
{
    inproc_bus<> bus;
    inproc_executor<> ex(bus);
    dialer d(ex, bus);
    channel_list accepted;
    auto listener = std::make_unique<inproc_transport<>>(ex, bus);
    listener->on_accepted(
            [&accepted, &listener](std::unique_ptr<inproc_channel<>> ch)
            {
                accepted.push_back(std::move(ch));
                listener.reset();
            });
    listener->listen(k_one);
    d.transport.dial(k_one);
    ex.drain();
    REQUIRE(accepted.size() == 1);
    REQUIRE(d.dialed.size() == 1);
    REQUIRE_FALSE(listener);

    d.transport.dial(k_one);
    ex.drain();
    REQUIRE(d.refused == error_list{io_error::connection_refused});
}
