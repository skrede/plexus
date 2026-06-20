// The transport_selector two-axis composition, as a PURE unit test (no socket, no
// io_context). The selector composes locality (the tier) x reliability (the
// scheme-encoded delivery class) FROM THE ENDPOINT ALONE — the dial(ep) reality,
// where the scheme is the only routing discriminator the engine path carries.
#pragma once

#include "plexus/io/endpoint.h"
#include "plexus/io/reliability.h"
#include "plexus/io/transport_selector.h"
#include "plexus/io/shm/dispatch_hint.h"
#include "plexus/io/reliability_requirement.h"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <string_view>

namespace pio = plexus::io;
