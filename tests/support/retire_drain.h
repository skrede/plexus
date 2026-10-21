#ifndef HPP_GUARD_PLEXUS_TESTS_SUPPORT_RETIRE_DRAIN_H
#define HPP_GUARD_PLEXUS_TESTS_SUPPORT_RETIRE_DRAIN_H

#include "plexus/io/byte_channel.h"

#include "plexus/io/detail/channel_graveyard.h"

#include "plexus/detail/compat.h"

#include <memory>
#include <cstdint>
#include <utility>
#include <concepts>

namespace plexus::testing {

struct retire_outcome
{
    std::int32_t erased = 0;
    bool empty          = false;
};

// Retires the channel through a real graveyard, which installs the erase as the channel's on_closed, then runs the
// caller's drain; on_erased runs right after each erase. A channel that honors the close contract reports exactly one
// erase and an empty graveyard.
template<typename Ch, typename Drain>
    requires io::byte_channel<Ch> && std::invocable<Drain &>
retire_outcome retire_and_drain(std::unique_ptr<Ch> &ch, Drain drain, plexus::detail::move_only_function<void()> on_erased = {})
{
    retire_outcome outcome;
    io::detail::channel_graveyard<Ch, 1> graveyard;

    graveyard.retire(std::move(ch),
                     [&outcome, &on_erased]
                     {
                         ++outcome.erased;
                         if(on_erased)
                             on_erased();
                     });
    drain();
    outcome.empty = graveyard.empty();
    return outcome;
}

}

#endif
