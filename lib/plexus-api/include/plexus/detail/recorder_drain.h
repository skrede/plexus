#ifndef HPP_GUARD_PLEXUS_DETAIL_RECORDER_DRAIN_H
#define HPP_GUARD_PLEXUS_DETAIL_RECORDER_DRAIN_H

#include "plexus/recorder_options.h"

#include "plexus/detail/compat.h"

#include "plexus/io/recording/flat_recorder.h"
#include "plexus/io/recording/pre_buffer_controller.h"

#include <memory>
#include <variant>
#include <cstddef>
#include <utility>

namespace plexus::detail {

// The recording state lives behind a stable heap address so a posted drain task captured once at
// attach survives a move of the owning handle. The drain re-arms on events: a posted task drains a
// bounded batch and re-posts itself only while the ring still holds records; once empty it goes
// quiescent and the next pushed edge re-arms it via kick().
struct recorder_block
{
    using mode_variant = std::variant<io::recording::flat_recorder, io::recording::pre_buffer_controller>;

    recorder_block(io::recording::byte_sink &sink, const recorder_options &opts, io::recording::flat_recorder::clock_fn clock, std::size_t scratch_bytes)
            : machinery(make_machinery(sink, opts, std::move(clock), scratch_bytes))
            , draining(false)
            , armed(false)
    {
    }

    static mode_variant make_machinery(io::recording::byte_sink &sink, const recorder_options &opts, io::recording::flat_recorder::clock_fn clock, std::size_t scratch_bytes)
    {
        if(opts.mode == recording_mode::pre_buffer)
            return mode_variant{std::in_place_type<io::recording::pre_buffer_controller>, sink, opts.ring_bytes, std::move(clock), opts.drain_batch_bytes};
        return mode_variant{std::in_place_type<io::recording::flat_recorder>, sink, opts.ring_bytes, std::move(clock), opts.drain_batch_bytes, scratch_bytes};
    }

    io::recording::flat_recorder &flat()
    {
        return std::get<io::recording::flat_recorder>(machinery);
    }

    io::recording::pre_buffer_controller &pre()
    {
        return std::get<io::recording::pre_buffer_controller>(machinery);
    }

    bool pump()
    {
        return std::visit([](auto &m) { return m.pump(); }, machinery);
    }

    void flush()
    {
        std::visit([](auto &m) { m.flush(); }, machinery);
    }

    // The caller deregisters the observer first, so no record lands while the ring empties.
    void drain_to_sink()
    {
        draining = false;
        while(pump())
        {
        }
        flush();
    }

    void kick()
    {
        if(draining && !armed && rearm)
        {
            armed = true;
            rearm();
        }
    }

    mode_variant machinery;
    plexus::detail::move_only_function<void()> rearm;
    bool draining;
    bool armed;
};

template<typename Policy, typename Executor>
void post_recorder_drain(const std::shared_ptr<recorder_block> &blk, Executor exec)
{
    std::weak_ptr<recorder_block> weak = blk;
    Policy::post(exec,
                 [weak = std::move(weak), exec]() mutable
                 {
                     auto live = weak.lock();
                     if(!live || !live->draining)
                         return;
                     if(live->pump())
                     {
                         post_recorder_drain<Policy>(live, exec);
                         return;
                     }
                     live->armed = false;
                 });
}
}

#endif
