#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_policy.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/wire_capturing_transport.h"

#include <catch2/catch_test_macros.hpp>

#include <memory>
#include <vector>
#include <utility>

using plexus::io::endpoint;
using plexus::inproc::inproc_bus;
using plexus::inproc::inproc_policy;
using plexus::inproc::inproc_executor;
using plexus::inproc::inproc_transport;

namespace {

using capture_t = plexus::io::wire_capturing_transport<inproc_transport<>, inproc_policy>;
using accepted  = std::vector<std::unique_ptr<capture_t::channel_type>>;

const endpoint k_listen{"inproc", "wire-capture"};

}

TEST_CASE("wire_capture: a capture destroyed before its inner transport leaves the inner accept sink empty", "[integration][wire_capture]")
{
    inproc_bus<> bus;
    inproc_executor<> ex(bus);
    inproc_transport<> inner(ex, bus);
    inproc_transport<> dialer(ex, bus);
    accepted got;
    auto capture = std::make_unique<capture_t>(inner);
    capture->on_accepted([&got](std::unique_ptr<capture_t::channel_type> ch) { got.push_back(std::move(ch)); });
    capture->listen(k_listen);
    capture.reset();

    dialer.dial(k_listen);
    REQUIRE(inner.retiring_size() == 1);
    REQUIRE(got.empty());
    ex.drain();
}

TEST_CASE("wire_capture: clearing the accept sink clears the inner transport's, so the inner retires the channel", "[integration][wire_capture]")
{
    inproc_bus<> bus;
    inproc_executor<> ex(bus);
    inproc_transport<> inner(ex, bus);
    inproc_transport<> dialer(ex, bus);
    accepted got;
    capture_t capture(inner);
    capture.on_accepted([&got](std::unique_ptr<capture_t::channel_type> ch) { got.push_back(std::move(ch)); });
    capture.on_accepted(nullptr);
    capture.listen(k_listen);

    dialer.dial(k_listen);
    REQUIRE(inner.retiring_size() == 1);
    REQUIRE(got.empty());
    ex.drain();
}
