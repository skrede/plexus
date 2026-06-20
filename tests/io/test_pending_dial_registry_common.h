#ifndef HPP_GUARD_PLEXUS_TESTS_IO_TEST_PENDING_DIAL_REGISTRY_COMMON_H
#define HPP_GUARD_PLEXUS_TESTS_IO_TEST_PENDING_DIAL_REGISTRY_COMMON_H

// The pending_dial_registry oracle: a pure sans-IO drive of the generic half-open
// dial table + accepted table + ownership-transfer, with a destruction-counting
// stand-in Channel (a recording POD, no socket and no backend link — plexus::plexus
// only).

#include "plexus/io/pending_dial_registry.h"

#include <catch2/catch_test_macros.hpp>

#include <memory>
#include <string>
#include <vector>
#include <utility>

namespace pending_dial_registry_fixture {

// A recording stand-in channel: bumps a shared counter on destruction so the test can
// observe exactly WHEN (synchronously vs. deferred) a freed channel is torn down.
struct fake_channel
{
    int *destroyed;
    int  id;

    explicit fake_channel(int *counter, int identity)
            : destroyed(counter)
            , id(identity)
    {
    }

    ~fake_channel() { ++*destroyed; }

    fake_channel(const fake_channel &)            = delete;
    fake_channel &operator=(const fake_channel &) = delete;
};

using registry = plexus::io::pending_dial_registry<fake_channel>;

// A defer-destroy sink that PARKS the freed channel instead of destroying it, so the
// test can assert the channel survives fail() and is torn down only when the park is
// released (the deferred edge — what a posted continuation does on the real path).
struct deferred_sink
{
    std::vector<std::unique_ptr<fake_channel>> parked;

    registry::defer_destroy callback()
    {
        return [this](std::unique_ptr<fake_channel> ch) { parked.push_back(std::move(ch)); };
    }

    void run() { parked.clear(); }
};

}

#endif
