#ifndef HPP_GUARD_BENCHMARKS_LOOPBACK_SELF_LOOPBACK_LANE_INTRA_H
#define HPP_GUARD_BENCHMARKS_LOOPBACK_SELF_LOOPBACK_LANE_INTRA_H

#include "self_loopback_report.h"

#include "plexus/expected.h"
#include "plexus/publisher.h"
#include "plexus/subscriber.h"
#include "plexus/typed_codec.h"
#include "plexus/loopback_node.h"
#include "plexus/node_options.h"

#include "plexus/io/message_info.h"
#include "plexus/io/process_loopback_channel.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_timer.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"

#include "plexus/discovery/static_discovery.h"

#include <span>
#include <atomic>
#include <memory>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <cstdlib>
#include <iostream>

namespace self_loopback {

// The pure intra-node Policy: a single transport binds the loopback channel concretely, so a
// same-node typed publish delivers by address with the codec's encode never invoked.
struct intra_policy
{
    using executor_type     = plexus::inproc::inproc_executor<> &;
    using byte_channel_type = plexus::io::process_loopback_channel<intra_policy>;
    using timer_type        = plexus::inproc::inproc_timer<>;
    using byte_owner        = std::shared_ptr<const void>;

    static void post(executor_type ex, plexus::detail::move_only_function<void()> fn)
    {
        ex.post(std::move(fn));
    }
};

// A timestamp-carrying typed value plus a codec whose encode count is shared, so the bench
// witnesses encodes==0 on the in-process object lane regardless of which codec copy delivers.
struct stamped
{
    std::uint64_t at{};
};

struct stamped_codec
{
    using value_type                          = stamped;
    std::shared_ptr<std::atomic<int>> encodes = std::make_shared<std::atomic<int>>(0);

    plexus::wire_bytes<> encode(const stamped &v) const
    {
        ++*encodes;
        auto owner = std::make_shared<std::vector<std::byte>>(sizeof v.at);
        write_stamp(*owner, v.at);
        return {std::span<const std::byte>{owner->data(), owner->size()}, std::move(owner)};
    }

    plexus::expected<void, std::error_code> decode(std::span<const std::byte> b, stamped &out) const
    {
        if(b.size() != sizeof out.at)
            return plexus::expected<void, std::error_code>{plexus::unexpect, std::make_error_code(std::errc::invalid_argument)};
        out.at = read_stamp(b);
        return {};
    }

    plexus::type_identity type_info() const { return {0x70105A11u, "stamped"}; }
};

inline plexus::node_id intra_id()
{
    plexus::node_id id{};
    id[0] = std::byte{0x70};
    return id;
}

// Drive the typed zero-copy point: borrow → stamp → publish the object, drain, capture the
// one-way delta. The codec's encode MUST stay at zero (the witnessed zero-copy proof).
inline cell intra_typed_point(std::size_t payload, std::uint64_t messages)
{
    plexus::inproc::inproc_bus<>            bus;
    plexus::inproc::inproc_executor<>       ex{bus};
    plexus::discovery::static_discovery     disc{{}};
    plexus::loopback_host<intra_policy>     host{ex, disc, intra_id()};

    stamped_codec       codec;
    auto                encodes = codec.encodes;
    std::vector<double> samples;
    plexus::subscriber<stamped_codec> sub{host.node(), "bench/typed", [&](const stamped &v, const plexus::io::message_info &)
                                          { samples.push_back(static_cast<double>(now_count() - v.at) / 1000.0); }};
    plexus::publisher<stamped_codec>  pub{host.node(), "bench/typed", plexus::typed_publisher_options{}, codec};
    ex.drain();

    const auto t0 = clock_type::now();
    for(std::uint64_t i = 0; i < messages; ++i)
    {
        auto loan = pub.borrow();
        loan->at  = now_count();
        pub.publish(std::move(loan));
        ex.drain();
    }
    const double secs = std::chrono::duration<double>(clock_type::now() - t0).count();

    if(encodes->load() != 0)
    {
        std::cerr << "FATAL: intra-node typed lane encoded " << encodes->load() << " times (expected 0)\n";
        std::abort();
    }
    (void)payload;
    return reduce(samples, secs);
}

// The intra-node bytes lane: a bytes publisher/subscriber on the same loopback node, the framed
// self-route. Sweeps the payload (a stamp is written into the leading bytes of each frame).
inline cell intra_bytes_point(std::size_t payload, std::uint64_t messages)
{
    plexus::inproc::inproc_bus<>        bus;
    plexus::inproc::inproc_executor<>   ex{bus};
    plexus::discovery::static_discovery disc{{}};
    plexus::loopback_host<intra_policy> host{ex, disc, intra_id()};

    std::vector<double>  samples;
    plexus::subscriber<> sub{host.node(), "bench/bytes", [&](std::span<const std::byte> b)
                             { samples.push_back(static_cast<double>(now_count() - read_stamp(b)) / 1000.0); }};
    plexus::publisher<>  pub{host.node(), "bench/bytes"};
    ex.drain();

    std::vector<std::byte> frame(std::max<std::size_t>(payload, sizeof(std::uint64_t)));
    const auto             t0 = clock_type::now();
    for(std::uint64_t i = 0; i < messages; ++i)
    {
        write_stamp(frame, now_count());
        pub.publish(std::span<const std::byte>{frame});
        ex.drain();
    }
    return reduce(samples, std::chrono::duration<double>(clock_type::now() - t0).count());
}

}

#endif
