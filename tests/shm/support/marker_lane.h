#ifndef HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_MARKER_LANE_H
#define HPP_GUARD_PLEXUS_TESTS_SHM_SUPPORT_MARKER_LANE_H

#include "support/hold_probe.h"
#include "support/co_host_node.h"
#include "support/region_names.h"

#include "plexus/asio/shm/shm_member.h"

#include "plexus/native/shm_region_broker.h"

#include "plexus/shm/loan_status.h"
#include "plexus/shm/region_naming.h"

#include "plexus/io/congestion.h"
#include "plexus/io/reliability.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>

#include <span>
#include <array>
#include <memory>
#include <string>
#include <vector>
#include <cstddef>
#include <cstdint>

namespace plexus::testing {

// One shared-memory member that writes a reliable topic's ring and reads it back through a receive companion, so a case
// can hold, place and resume the reader the way a switching subscriber's is.
struct marker_lane
{
    explicit marker_lane(const std::string &topic)
            : io()
            , broker()
            , region(co_host_region("swm"))
            , member(plexus::asio::shm::make_shm_member(io, broker, plexus::io::reliability::reliable, plexus::io::congestion::block, region))
            , fqn(topic)
            , send()
            , receive()
            , probe()
            , delivered()
    {
        bind_test_publisher(member);
        send = member.mint_companion(fqn, {});
        REQUIRE(send);
        REQUIRE(probe.attach(region, fqn, member.self_id()));
    }

    void attach()
    {
        receive = member.mint_receive_companion(fqn, member.self_id(), {});
        REQUIRE(receive);
    }

    bool place()
    {
        return member.companion_place(fqn)(receive->reader_claim());
    }

    void resume()
    {
        receive->resume([this](std::span<const std::byte> bytes) { delivered.push_back(index_of(bytes)); });
    }

    plexus::shm::loan_status publish(std::uint32_t index)
    {
        const std::array<std::byte, 4> bytes = index_bytes(index);
        return member.registry().channel_for(fqn, plexus::shm::ring_direction::request, member.self_id())->send(bytes);
    }

    // Runs every wake the ring has posted.
    void pump()
    {
        io.restart();
        io.poll();
    }

    std::uint64_t reader_position()
    {
        return probe.position(receive->reader_claim().slot);
    }

    std::uint32_t cells() const
    {
        return static_cast<std::uint32_t>(probe.ring.cell_count());
    }

    ::asio::io_context io;
    plexus::native::shm_region_broker broker;
    std::string region;
    plexus::asio::shm::shm_member member;
    std::string fqn;
    std::unique_ptr<plexus::asio::shm::shm_member::channel_type> send;
    std::unique_ptr<plexus::asio::shm::shm_member::consumer_type> receive;
    ring_probe probe;
    std::vector<std::uint32_t> delivered;
};

}

#endif
