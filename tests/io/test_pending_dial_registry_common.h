#ifndef HPP_GUARD_PLEXUS_TESTS_IO_TEST_PENDING_DIAL_REGISTRY_COMMON_H
#define HPP_GUARD_PLEXUS_TESTS_IO_TEST_PENDING_DIAL_REGISTRY_COMMON_H

#include "contract_channel.h"

#include "plexus/io/endpoint.h"
#include "plexus/io/io_error.h"
#include "plexus/io/pending_dial_registry.h"

#include "plexus/detail/compat.h"

#include <catch2/catch_test_macros.hpp>

#include <memory>
#include <string>
#include <vector>

namespace pending_dial_registry_fixture {

using plexus::testing::fifo_executor;
using plexus::testing::contract_channel;
using event_log        = std::vector<std::string>;
using registry         = plexus::io::pending_dial_registry<contract_channel>;
using dial_failed_sink = plexus::detail::move_only_function<void(const plexus::io::endpoint &, plexus::io::io_error)>;

inline std::unique_ptr<contract_channel> make_channel(fifo_executor &executor, event_log &log)
{
    return std::make_unique<contract_channel>(executor, log);
}

inline plexus::io::endpoint dial_target()
{
    return {"tcp", "127.0.0.1:7"};
}

inline dial_failed_sink log_reports(event_log &log)
{
    return [&log](const plexus::io::endpoint &target, plexus::io::io_error e)
    { log.push_back((e == plexus::io::io_error::operation_aborted ? "aborted " : "failed ") + target.address); };
}

}

#endif
