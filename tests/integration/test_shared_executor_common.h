#ifndef HPP_GUARD_TESTS_INTEGRATION_SHARED_EXECUTOR_COMMON_H
#define HPP_GUARD_TESTS_INTEGRATION_SHARED_EXECUTOR_COMMON_H

#include "plexus/mdnspp/mdnspp_discovery.h"

#include "plexus/asio/asio_policy.h"
#include "plexus/asio/asio_channel.h"
#include "plexus/asio/asio_listener.h"

#include "plexus/io/message_forwarder.h"
#include "plexus/io/frame_router.h"

#include "plexus/wire/data_frame.h"

#include "plexus/discovery/discovery.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>

#include <span>
#include <chrono>
#include <vector>
#include <memory>
#include <string>
#include <cstddef>
#include <utility>
#include <optional>

namespace pasio = plexus::asio;
namespace wire  = plexus::wire;
namespace pio   = plexus::io;
namespace pmdns = plexus::mdnspp;

namespace shared_executor_fixture {

inline std::vector<std::byte> bytes_of(std::string_view s)
{
    std::vector<std::byte> v(s.size());
    for(std::size_t i = 0; i < s.size(); ++i)
        v[i] = static_cast<std::byte>(static_cast<unsigned char>(s[i]));
    return v;
}

inline std::string read_card_value(const std::vector<std::pair<std::string, std::string>> &card,
                                   std::string_view                                        key)
{
    for(const auto &[k, v] : card)
        if(k == key)
            return v;
    return {};
}

}

#endif
