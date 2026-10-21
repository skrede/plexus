#ifndef HPP_GUARD_PLEXUS_TESTS_IO_TEST_PENDING_DIAL_REGISTRY_COMMON_H
#define HPP_GUARD_PLEXUS_TESTS_IO_TEST_PENDING_DIAL_REGISTRY_COMMON_H

#include "contract_channel.h"

#include "plexus/io/pending_dial_registry.h"

#include <catch2/catch_test_macros.hpp>

#include <memory>
#include <string>
#include <vector>

namespace pending_dial_registry_fixture {

using plexus::testing::fifo_executor;
using plexus::testing::contract_channel;
using event_log = std::vector<std::string>;
using registry  = plexus::io::pending_dial_registry<contract_channel>;

inline std::unique_ptr<contract_channel> make_channel(fifo_executor &executor, event_log &log)
{
    return std::make_unique<contract_channel>(executor, log);
}

}

#endif
