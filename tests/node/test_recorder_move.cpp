// A recorder is move-constructed when stored in a container (the documented make_recorder ->
// push_back idiom). The shared drain block survives the move; its rearm closure must reach a live
// executor, never a member of the destroyed moved-from source.
//
// The first case drives the public API over the real node through a vector reallocation and recovers
// the captured sample. The second instantiates the recorder with a BY-VALUE executor_type — the only
// shape under which the pre-fix closure (which aliased &m_executor) read a freed executor after the
// source was destroyed; every in-tree Policy uses a reference executor_type whose referent outlives
// the handle, so this case makes the regression fail if the fix is reverted (ASan is the gate).

#include "plexus/recorder.h"
#include "plexus/publisher.h"
#include "plexus/subscriber.h"

#include "plexus/io/observer.h"
#include "plexus/io/capture_policy.h"
#include "plexus/io/recording/byte_sink.h"
#include "plexus/io/recording/record_stream_reader.h"

#include "plexus/wire/topic_hash.h"

#include "plexus/detail/compat.h"
#include "plexus/node_id.h"

#include "test_self_loopback_common.h"

#include <catch2/catch_test_macros.hpp>

#include <span>
#include <array>
#include <vector>
#include <memory>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <utility>

using plexus::io::recording::record_category;
using plexus::io::recording::decoded_record;
using plexus::io::recording::recovery_result;
using plexus::io::recording::stream_definitions;
using plexus::io::recording::record_stream_reader;
using plexus_test::fixture;

namespace {

class memory_sink final : public plexus::io::recording::byte_sink
{
public:
    void write(std::span<const std::byte> bytes) override
    {
        m_bytes.insert(m_bytes.end(), bytes.begin(), bytes.end());
    }
    std::span<const std::byte> bytes() const noexcept
    {
        return m_bytes;
    }

private:
    std::vector<std::byte> m_bytes;
};

}

TEST_CASE("recorder_move: a recorder relocated by a vector reallocation still drains", "[node][recorder]")
{
    fixture f;
    memory_sink sink;
    memory_sink filler_sink;

    {
        using rec_t = decltype(f.node().make_recorder(sink));
        std::vector<rec_t> recs;
        recs.reserve(1);

        plexus::recorder_options ro;
        ro.ring_bytes = 1u << 20;
        recs.push_back(f.node().make_recorder(sink, std::move(ro)));

        std::vector<std::vector<std::byte>> seen;
        plexus::subscriber<> s{f.node(), "topic", [&](std::span<const std::byte> b) { seen.emplace_back(b.begin(), b.end()); }};
        plexus::publisher<> p{f.node(), "topic"};
        f.drive();

        // A second recorder crosses the capacity-1 boundary: element 0 is move-constructed into the
        // grown buffer and its old storage destroyed. The surviving handle must still drain.
        plexus::recorder_options ro2;
        ro2.ring_bytes = 1u << 20;
        recs.push_back(f.node().make_recorder(filler_sink, std::move(ro2)));
        f.drive();

        const std::array<std::byte, 4> mk{std::byte{0xA0}, std::byte{0x01}, std::byte{0xBE}, std::byte{0xEF}};
        p.publish(mk);
        f.drive();

        REQUIRE(seen.size() == 1);
        f.drive();
        recs[0].flush();
    }
    f.drive();

    record_stream_reader reader{sink.bytes()};
    stream_definitions defs;
    REQUIRE(reader.read_definitions(defs));

    std::vector<decoded_record> records;
    const recovery_result res = reader.recover(records);
    REQUIRE(res.header_ok);

    bool saw_topic = false;
    for(const auto &r : records)
        if(r.category == record_category::sample && r.topic_hash == plexus::wire::fqn_topic_hash("topic"))
            saw_topic = true;
    REQUIRE(saw_topic);
}

namespace {

// A by-value, copyable executor handle: the shape a user Policy supplies when its executor_type is
// not a reference. post() forwards to a shared queue the test drives by hand.
struct value_executor
{
    std::vector<plexus::detail::move_only_function<void()>> *queue{};
};

struct value_executor_policy
{
    using executor_type = value_executor;

    static void post(executor_type ex, plexus::detail::move_only_function<void()> fn)
    {
        ex.queue->push_back(std::move(fn));
    }
};

struct stub_messages
{
    std::optional<std::uint64_t> producer_type_id(std::uint64_t) const
    {
        return std::nullopt;
    }
};

// The minimal engine surface the recorder consults: an observer registry and the two per-sample
// resolvers. It is never a routing_engine — the point is a by-value executor_type.
struct value_engine
{
    using executor_type = value_executor;

    plexus::io::capture_policy m_capture;
    stub_messages m_messages;

    plexus::io::capture_policy &capture() noexcept
    {
        return m_capture;
    }
    stub_messages &messages() noexcept
    {
        return m_messages;
    }
    void add_observer(plexus::io::observer &) noexcept
    {
    }
    void remove_observer(plexus::io::observer &) noexcept
    {
    }
};

}

TEST_CASE("recorder_move: a by-value executor is owned by the drain closure across a move", "[node][recorder]")
{
    std::vector<plexus::detail::move_only_function<void()>> queue;
    memory_sink sink;
    value_engine engine;
    plexus::node_id id{};

    using rec_t = plexus::recorder<value_engine, value_executor_policy>;
    std::vector<rec_t> recs;
    {
        plexus::recorder_options ro;
        ro.mode = plexus::recording_mode::pre_buffer;
        // Move element 0 out of a heap source, then free the heap block: a pre-fix closure captured
        // &source->m_executor and now dangles; the fixed closure owns a copy of the handle.
        auto source = std::make_unique<rec_t>(engine, value_executor{&queue}, id, sink, std::move(ro), plexus::wire_crypto_position::cleartext);
        recs.push_back(std::move(*source));
        source.reset();
    }

    recs[0].trigger();

    while(!queue.empty())
    {
        auto fn = std::move(queue.front());
        queue.erase(queue.begin());
        fn();
    }

    SUCCEED("the drain closure reached a live executor after the source was destroyed");
}
