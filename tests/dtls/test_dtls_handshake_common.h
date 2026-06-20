#ifndef HPP_GUARD_PLEXUS_TESTS_DTLS_TEST_DTLS_HANDSHAKE_COMMON_H
#define HPP_GUARD_PLEXUS_TESTS_DTLS_TEST_DTLS_HANDSHAKE_COMMON_H

#include "dtls_test_support.h"

#include "plexus/tls/dtls_channel.h"
#include "plexus/tls/dtls_cookie.h"
#include "plexus/tls/dtls_transport.h"

#include "plexus/asio/udp_server.h"

#include <catch2/catch_test_macros.hpp>

#include <asio/io_context.hpp>
#include <asio/ip/udp.hpp>

#include <span>
#include <memory>
#include <string>
#include <vector>
#include <chrono>
#include <cstring>
#include <cstddef>

namespace pdt   = plexus::dtls_test;
namespace ptls  = plexus::tls;
namespace pasio = plexus::asio;

#endif
