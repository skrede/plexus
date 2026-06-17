#ifndef HPP_GUARD_PLEXUS_API_RECORDER_H
#define HPP_GUARD_PLEXUS_API_RECORDER_H

#include "plexus/recorder_options.h"

#include "plexus/io/observer.h"
#include "plexus/io/capture_policy.h"
#include "plexus/io/recording/byte_sink.h"
#include "plexus/io/recording/flat_recorder.h"
#include "plexus/io/recording/recording_sink.h"
#include "plexus/io/recording/record_envelope.h"
#include "plexus/io/recording/pre_buffer_controller.h"

#include "plexus/wire/frame_codec.h"

#include "plexus/detail/compat.h"
#include "plexus/node_id.h"

#include <span>
#include <memory>
#include <variant>
#include <cstddef>
#include <cstdint>
#include <utility>

namespace plexus {

// The RAII recorder handle returned by node.make_recorder — the public attach verb, a
// sibling of the publisher/subscriber/procedure endpoint family. It owns the recording
// machinery (the continuous flat_recorder or the pre_buffer FDR controller over one ring)
// and the recording_sink observer that feeds it. On construction it registers the sink on
// the engine (add_observer auto-feeds the capture gate's observer-presence — the always-on
// metadata floor) and, in continuous mode, emits the stream header + Definitions preamble.
//
// LIFETIME (the load-bearing teardown discipline): the dtor DEREGISTERS the sink before
// any engine/executor teardown — mirroring ~node's remove-before-teardown — so no posted
// drain closure outlives the ring it reads. The cooperative drain is a self-re-posting
// task riding the consumer's run-loop (no mandatory plexus thread); it captures a weak
// handle to a heap drain block, so an in-flight task that fires after the recorder is gone
// finds the block expired and stops, never touching freed state. The dtor drains the ring
// out synchronously, then deregisters, then drops the block — the post-dtor pump is inert.
//
// Move-only (structural single-owner; no copy, no shared_from_this, no raw pointers in the
// surface). The heap drain block keeps the ring's address stable across a move so the
// already-posted drain task stays valid.
template <typename Engine, typename Policy>
class recorder
{
    // The recording state lives behind a stable heap address so a posted drain task —
    // captured once at attach — survives a move of the owning handle. It is held by
    // shared_ptr so an in-flight posted task (holding a weak_ptr) can detect teardown. The
    // drain is EVENT-DRIVEN re-arm (the egress-scheduler precedent): a posted task drains a
    // bounded batch and re-posts ITSELF only while the ring still holds records; once the
    // ring empties the task goes quiescent and the next pushed edge re-arms it (kick()). An
    // unconditional self-re-post would starve a cooperative single-threaded executor.
    struct block
    {
        using mode_variant =
            std::variant<io::recording::flat_recorder, io::recording::pre_buffer_controller>;

        block(io::recording::byte_sink &sink, const recorder_options &opts,
              io::recording::flat_recorder::clock_fn clock)
            : machinery(make_machinery(sink, opts, std::move(clock)))
        {
        }

        static mode_variant make_machinery(io::recording::byte_sink &sink,
                                            const recorder_options &opts,
                                            io::recording::flat_recorder::clock_fn clock)
        {
            if(opts.mode == recording_mode::pre_buffer)
                return mode_variant{std::in_place_type<io::recording::pre_buffer_controller>,
                                    sink, opts.ring_bytes, std::move(clock), opts.drain_batch_bytes};
            return mode_variant{std::in_place_type<io::recording::flat_recorder>,
                                sink, opts.ring_bytes, std::move(clock), opts.drain_batch_bytes};
        }

        bool pump()
        {
            return std::visit([](auto &m) { return m.pump(); }, machinery);
        }

        // Re-arm the cooperative drain if it is quiescent — called from the observer turn
        // after a push, the event-driven resume.
        void kick()
        {
            if(draining && !armed && rearm)
            {
                armed = true;
                rearm();
            }
        }

        mode_variant                                    machinery;
        plexus::detail::move_only_function<void()>      rearm;
        bool                                            draining{false};
        bool                                            armed{false};
    };

public:
    using anomaly_predicate = recorder_options::anomaly_predicate;

    // Constructed only by node.make_recorder. The engine + executor are borrowed (never
    // owned); the consumer's byte_sink is borrowed too and MUST outlive the recorder.
    recorder(Engine &engine, typename Engine::executor_type executor,
             const node_id &id, io::recording::byte_sink &sink, recorder_options opts)
        : m_engine(&engine)
        , m_executor(executor)
        , m_block(std::make_shared<block>(sink, opts,
                  io::recording::flat_recorder::clock_fn{[] { return wire::now_timestamp_ns(); }}))
        , m_sink(make_sink())
    {
        if(opts.mode == recording_mode::pre_buffer && opts.on_anomaly)
            pre().on_anomaly(std::move(*opts.on_anomaly));
        if(opts.mode == recording_mode::continuous)
            flat().open(id, m_engine->capture().default_rule());
        m_block->draining = true;
        m_block->rearm = [blk = std::weak_ptr<block>(m_block), exec = &m_executor] {
            if(auto live = blk.lock())
                post_drain(live, exec);
        };
        m_engine->add_observer(*m_sink);
    }

    recorder(const recorder &) = delete;
    recorder &operator=(const recorder &) = delete;

    recorder(recorder &&other) noexcept
        : m_engine(other.m_engine)
        , m_executor(other.m_executor)
        , m_block(std::move(other.m_block))
        , m_sink(std::move(other.m_sink))
    {
        other.m_engine = nullptr;
    }

    recorder &operator=(recorder &&) = delete;

    // The teardown discipline: deregister the sink FIRST (no further edge enters the ring),
    // drain whatever the ring still holds out to the sink, then drop the drain block so any
    // already-posted drain task finds it expired and exits without touching freed state. A
    // dtor must not throw — every step is non-throwing or wrapped.
    ~recorder()
    {
        if(m_engine == nullptr)
            return;
        try
        {
            m_engine->remove_observer(*m_sink);
            if(m_block)
            {
                m_block->draining = false;
                while(m_block->pump())
                {
                }
                std::visit([](auto &m) { m.flush(); }, m_block->machinery);
            }
        }
        catch(...)
        {
        }
    }

    // The FDR surface (pre_buffer mode). trigger() freezes the held window (two index
    // reads, alloc-free); captured_span() reports newest_ts - oldest_ts over it. In
    // continuous mode trigger() is a no-op and the span is 0.
    void trigger() noexcept
    {
        if(auto *p = std::get_if<io::recording::pre_buffer_controller>(&m_block->machinery))
        {
            p->trigger();
            m_block->kick(); // re-arm the cooperative drain to ship the frozen window out
        }
    }

    [[nodiscard]] std::uint64_t captured_span() const noexcept
    {
        if(const auto *p = std::get_if<io::recording::pre_buffer_controller>(&m_block->machinery))
            return p->captured_span();
        return 0;
    }

    // Drive one bounded cooperative drain batch manually; returns true while work remains.
    // The self-re-posting task calls this on the consumer's turns — a consumer may also
    // call it directly for explicit drain control.
    bool pump() { return m_block->pump(); }

    void flush() { std::visit([](auto &m) { m.flush(); }, m_block->machinery); }

private:
    io::recording::flat_recorder         &flat() { return std::get<io::recording::flat_recorder>(m_block->machinery); }
    io::recording::pre_buffer_controller &pre()  { return std::get<io::recording::pre_buffer_controller>(m_block->machinery); }

    // The observer forwards each edge to whichever discipline the variant holds.
    std::unique_ptr<io::observer> make_sink()
    {
        return std::make_unique<variant_sink>(*m_block);
    }

    // Post one cooperative drain turn. It re-posts ITSELF only while the ring still holds
    // records; when the ring empties it clears `armed` and goes quiescent (the next pushed
    // edge re-arms it through block::kick) — so it never starves a cooperative executor. It
    // holds a weak handle to the drain block; once the recorder is gone the lock fails and
    // the task exits, never reading a freed ring (the teardown-UAF discipline).
    // The executor is a reference type (inproc_executor& / io_context&); capture it as a
    // plain pointer so the closure stays trivially copyable (capturing the reference-typed
    // value by-value would try to COPY the executor). The executor is the node's, borrowed
    // for the node's life — it outlives every drain task by the teardown contract.
    using executor_ptr = std::remove_reference_t<typename Engine::executor_type> *;

    static void post_drain(const std::shared_ptr<block> &blk, executor_ptr exec)
    {
        std::weak_ptr<block> weak = blk;
        Policy::post(*exec, [weak = std::move(weak), exec]() mutable {
            auto live = weak.lock();
            if(!live || !live->draining)
                return;
            if(live->pump())
            {
                post_drain(live, exec); // work remains — keep draining on later turns
                return;
            }
            live->armed = false; // ring empty — go quiescent until the next edge kicks
        });
    }

    // A small observer that dispatches every posted edge to the variant-held recorder. It
    // mirrors recording_sink's edge set but over the std::variant (the two disciplines
    // share the record_* contract, visited without virtual indirection on the build turn).
    class variant_sink : public io::observer
    {
    public:
        explicit variant_sink(block &blk) noexcept : m_block(blk) {}

        bool observes_data_path() const override { return true; }

        void on_message_published(std::string_view fqn, const io::message_view &v) override
        {
            sample(fqn, m_empty_info, v);
        }
        void on_message_delivered(std::string_view fqn, const io::message_info &info,
                                  const io::message_view &v) override
        {
            sample(fqn, info, v);
        }
        void on_drop(const io::detail::drop_event &e) override
        {
            record([&](auto &m) { m.record_drop(e); });
        }
        void on_qos_change(const io::qos_change_event &e) override
        {
            record([&](auto &m) { m.record_qos_change(e); });
        }
        void on_participant(const io::participant_event &e) override
        {
            record([&](auto &m) { m.record_participant(e); });
        }
        void on_endpoint(std::string_view fqn, const io::endpoint_event &e) override
        {
            record([&](auto &m) { m.record_endpoint(fqn, e); });
        }
        void on_security(const io::security_event &e) override
        {
            record([&](auto &m) { m.record_security(e); });
        }
        void on_wire(const io::recording::wire_record &rec) override
        {
            record([&](auto &m) { m.record_wire(rec.dir, rec.seq, rec.peer, rec.bytes); });
        }

    private:
        void sample(std::string_view fqn, const io::message_info &info, const io::message_view &v)
        {
            const std::span<const std::byte> bytes = v;
            record([&](auto &m) {
                m.record_sample(wire::fqn_topic_hash(fqn), info, 0u, false,
                                io::capture_fidelity::payload, bytes);
            });
        }

        // Encode the edge into whichever discipline the variant holds, then re-arm the
        // cooperative drain (event-driven resume — the quiescent drain wakes on the push).
        template <typename Fn>
        void record(Fn &&fn)
        {
            std::visit(std::forward<Fn>(fn), m_block.machinery);
            m_block.kick();
        }

        block             &m_block;
        const io::message_info m_empty_info{};
    };

    Engine                       *m_engine;
    typename Engine::executor_type m_executor;
    std::shared_ptr<block>        m_block;
    std::unique_ptr<io::observer> m_sink;
};

}

#endif
