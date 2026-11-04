#ifndef HPP_GUARD_PLEXUS_TESTS_FUZZ_FUZZ_SESSION_POLICY_H
#define HPP_GUARD_PLEXUS_TESTS_FUZZ_FUZZ_SESSION_POLICY_H

#include "fuzz_clock.h"

#include "plexus/io/peer_session.h"
#include "plexus/io/message_forwarder.h"
#include "plexus/io/procedure_forwarder.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_timer.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"
#include "plexus/inproc/inproc_transport.h"

#include "plexus/executor_verbs.h"

#include <memory>
#include <type_traits>

namespace fuzz_session {

using bus       = plexus::inproc::inproc_bus<fuzz_clock>;
using executor  = plexus::inproc::inproc_executor<fuzz_clock>;
using channel   = plexus::inproc::inproc_channel<fuzz_clock>;
using transport = plexus::inproc::inproc_transport<fuzz_clock>;

struct fuzz_policy : plexus::executor_verbs<executor>
{
    using byte_channel_type = channel;
    using timer_type        = plexus::inproc::inproc_timer<fuzz_clock>;
    using byte_owner        = std::shared_ptr<const void>;
};

using session       = plexus::io::peer_session<fuzz_policy>;
using msg_forwarder = plexus::io::message_forwarder<fuzz_policy>;
using rpc_forwarder = plexus::io::procedure_forwarder<fuzz_policy>;

static_assert(std::is_same_v<session::clock_type, fuzz_clock>);

}

#endif
