#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"

#include <catch2/catch_test_macros.hpp>

#include <memory>
#include <vector>
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
const endpoint k_two{"inproc", "two"};

struct probe
{
    probe(inproc_executor<> &ex, inproc_bus<> &bus)
            : errors()
            , refused()
            , dialed()
            , accepted()
            , transport(ex, bus)
    {
        transport.on_accepted([this](std::unique_ptr<inproc_channel<>> ch) { accepted.push_back(std::move(ch)); });
        transport.on_dialed([this](std::unique_ptr<inproc_channel<>> ch, const endpoint &) { dialed.push_back(std::move(ch)); });
        transport.on_dial_failed([this](const endpoint &, io_error e) { refused.push_back(e); });
        transport.on_error([this](io_error e) { errors.push_back(e); });
    }

    error_list errors;
    error_list refused;
    channel_list dialed;
    channel_list accepted;
    inproc_transport<> transport;
};

}

TEST_CASE("inproc listen: a transport that listens on two endpoints is dialable on both and leaves neither behind after close", "[inproc]")
{
    inproc_bus<> bus;
    inproc_executor<> ex(bus);
    probe a(ex, bus);
    probe b(ex, bus);
    a.transport.listen(k_one);
    a.transport.listen(k_two);
    b.transport.dial(k_one);
    b.transport.dial(k_two);
    ex.drain();
    REQUIRE(a.accepted.size() == 2);
    REQUIRE(b.dialed.size() == 2);

    a.transport.close();
    ex.drain();
    b.transport.dial(k_one);
    b.transport.dial(k_two);
    ex.drain();
    REQUIRE(b.refused == error_list{io_error::connection_refused, io_error::connection_refused});
    REQUIRE(a.accepted.size() == 2);
}

TEST_CASE("inproc listen: a second listen on an endpoint another transport serves is refused with address_in_use", "[inproc]")
{
    inproc_bus<> bus;
    inproc_executor<> ex(bus);
    probe a(ex, bus);
    probe b(ex, bus);
    probe c(ex, bus);
    a.transport.listen(k_one);
    c.transport.listen(k_one);
    b.transport.dial(k_one);
    ex.drain();
    REQUIRE(c.errors == error_list{io_error::address_in_use});
    REQUIRE(a.accepted.size() == 1);
    REQUIRE(c.accepted.empty());

    a.transport.close();
    b.transport.dial(k_one);
    ex.drain();
    REQUIRE(b.refused == error_list{io_error::connection_refused});
}

// The closing transport also tried the survivor's endpoint, so a close that erased by endpoint would take the
// survivor's registration with it.
TEST_CASE("inproc listen: closing one transport leaves another's listener dialable", "[inproc]")
{
    inproc_bus<> bus;
    inproc_executor<> ex(bus);
    probe b(ex, bus);
    probe c(ex, bus);
    auto a = std::make_unique<probe>(ex, bus);
    a->transport.listen(k_one);
    c.transport.listen(k_two);
    a->transport.listen(k_two);
    a->transport.close();
    a.reset();

    b.transport.dial(k_two);
    ex.drain();
    REQUIRE(c.accepted.size() == 1);
    REQUIRE(b.dialed.size() == 1);

    b.transport.dial(k_one);
    ex.drain();
    REQUIRE(b.refused == error_list{io_error::connection_refused});
}
