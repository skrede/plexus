#ifndef HPP_GUARD_PLEXUS_API_RECORDER_H
#define HPP_GUARD_PLEXUS_API_RECORDER_H

#include "plexus/recorder_options.h"
#include "plexus/wire_capture_qos.h"

#include "plexus/io/observer.h"
#include "plexus/io/capture_policy.h"
#include "plexus/io/recording/byte_sink.h"
#include "plexus/io/recording/flat_recorder.h"
#include "plexus/io/recording/recording_sink.h"
#include "plexus/io/recording/record_envelope.h"
#include "plexus/io/recording/record_stream_writer.h"
#include "plexus/io/recording/pre_buffer_controller.h"

#include "plexus/wire/varint.h"
#include "plexus/wire/frame_codec.h"

#include "plexus/detail/compat.h"
#include "plexus/node_id.h"

#include <span>
#include <vector>
#include <memory>
#include <variant>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <utility>
#include <algorithm>

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
template<typename Engine, typename Policy>
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
              io::recording::flat_recorder::clock_fn clock, std::size_t scratch_bytes)
                : machinery(make_machinery(sink, opts, std::move(clock), scratch_bytes))
        {
        }

        static mode_variant make_machinery(io::recording::byte_sink              &sink,
                                           const recorder_options                &opts,
                                           io::recording::flat_recorder::clock_fn clock,
                                           std::size_t                            scratch_bytes)
        {
            if(opts.mode == recording_mode::pre_buffer)
                return mode_variant{std::in_place_type<io::recording::pre_buffer_controller>, sink,
                                    opts.ring_bytes, std::move(clock), opts.drain_batch_bytes};
            return mode_variant{std::in_place_type<io::recording::flat_recorder>,
                                sink,
                                opts.ring_bytes,
                                std::move(clock),
                                opts.drain_batch_bytes,
                                scratch_bytes};
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

        mode_variant                               machinery;
        plexus::detail::move_only_function<void()> rearm;
        bool                                       draining{false};
        bool                                       armed{false};
    };

public:
    using anomaly_predicate = recorder_options::anomaly_predicate;

    // Constructed only by node.make_recorder. The engine + executor are borrowed (never
    // owned); the consumer's byte_sink is borrowed too and MUST outlive the recorder. The
    // crypto position is the node's per-transport wire-capture position, threaded through so
    // the stream preamble records which tap the wire capture used (a recording-only fact).
    recorder(Engine &engine, typename Engine::executor_type executor, const node_id &id,
             io::recording::byte_sink &sink, recorder_options opts, wire_crypto_position crypto)
            : m_engine(&engine)
            , m_executor(executor)
            , m_block(std::make_shared<block>(sink, opts,
                                              io::recording::flat_recorder::clock_fn{
                                                      [] { return wire::now_timestamp_ns(); }},
                                              preamble_scratch_bytes(opts.schemas)))
            , m_sink(make_sink())
    {
        if(opts.mode == recording_mode::pre_buffer && opts.on_anomaly)
            pre().on_anomaly(std::move(*opts.on_anomaly));
        if(opts.mode == recording_mode::continuous)
        {
            const auto rows = schema_rows(opts.schemas);
            // The on-disk ordinals of capture_crypto_position are pinned to the public
            // wire_crypto_position (cleartext=0, ciphertext=1), so the cast is a forward.
            flat().open(id, m_engine->capture().default_rule(), rows,
                        static_cast<io::recording::capture_crypto_position>(crypto));
        }
        m_block->draining = true;
        m_block->rearm    = [blk = std::weak_ptr<block>(m_block), exec = &m_executor]
        {
            if(auto live = blk.lock())
                post_drain(live, exec);
        };
        m_engine->add_observer(*m_sink);
    }

    recorder(const recorder &)            = delete;
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

    void flush()
    {
        std::visit([](auto &m) { m.flush(); }, m_block->machinery);
    }

private:
    io::recording::flat_recorder &flat()
    {
        return std::get<io::recording::flat_recorder>(m_block->machinery);
    }
    io::recording::pre_buffer_controller &pre()
    {
        return std::get<io::recording::pre_buffer_controller>(m_block->machinery);
    }

    // The observer forwards each edge to whichever discipline the variant holds, stamping
    // each sample with the topic's capture_policy-resolved fidelity (no longer hard-coded
    // payload) and its registered producer type_id (so an offline projector keys the sample
    // to the declared schema) — a wire-resolved tier on an inproc producer degrades to the
    // bare bytes too.
    std::unique_ptr<io::observer> make_sink()
    {
        return std::make_unique<variant_sink>(
                *m_block, [engine = m_engine](std::uint64_t hash)
                { return engine->capture().rule_for(hash).fidelity; },
                [engine = m_engine](std::uint64_t hash)
                { return engine->messages().producer_type_id(hash); });
    }

    // Translate the public schemas into the core preamble rows (field-for-field; the views
    // alias opts.schemas, which the caller keeps alive across this ctor — the open() below
    // copies the bytes into the stream synchronously).
    static std::vector<io::recording::type_schema_entry>
    schema_rows(const std::vector<type_schema> &schemas)
    {
        std::vector<io::recording::type_schema_entry> rows;
        rows.reserve(schemas.size());
        for(const type_schema &s : schemas)
            rows.push_back({s.type_id,
                            {},
                            s.message_encoding,
                            s.schema_name,
                            s.schema_encoding,
                            s.schema_data});
        return rows;
    }

    // The worst-case Definitions-preamble byte size for the declared schemas — used ONCE to
    // size the writer's bounds-check-free scratch so a multi-KiB schema blob cannot overrun
    // it (wire::writer is a raw memcpy). Sums each entry's worst-case varint-prefixed blobs
    // (type_name is always empty here) plus a fixed header margin, and floors at the default.
    static std::size_t preamble_scratch_bytes(const std::vector<type_schema> &schemas)
    {
        constexpr std::size_t k_default_scratch = 64u * 1024u;
        constexpr std::size_t k_header_margin   = 256u;
        auto                  blob = [](std::size_t n) { return wire::varint_size(n) + n; };
        std::size_t           need = k_header_margin + wire::varint_size(schemas.size());
        for(const type_schema &s : schemas)
            need += wire::varint_size(s.type_id) + blob(0) + blob(s.message_encoding.size()) +
                    blob(s.schema_name.size()) + blob(s.schema_encoding.size()) +
                    blob(s.schema_data.size());
        return std::max(k_default_scratch, need);
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
        Policy::post(*exec,
                     [weak = std::move(weak), exec]() mutable
                     {
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
        using type_id_resolver =
                plexus::detail::move_only_function<std::optional<std::uint64_t>(std::uint64_t)>;

        // The fidelity resolver maps a topic hash to its capture_policy-resolved tier so a
        // recorded sample is stamped with its true fidelity (unset falls back to payload);
        // the type_id resolver maps it to its registered producer type_id so the sample keys
        // to the declared schema (absent => 0/not-present, a valid opaque sample).
        variant_sink(
                block                                                                  &blk,
                plexus::detail::move_only_function<io::capture_fidelity(std::uint64_t)> fidelity,
                type_id_resolver                                                        type_id)
                : m_block(blk)
                , m_fidelity_resolver(std::move(fidelity))
                , m_type_id_resolver(std::move(type_id))
        {
        }

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
            const std::uint64_t              hash  = wire::fqn_topic_hash(fqn);
            const std::span<const std::byte> bytes = v;
            const io::capture_fidelity       fidelity =
                    m_fidelity_resolver ? m_fidelity_resolver(hash) : io::capture_fidelity::payload;
            const std::optional<std::uint64_t> id =
                    m_type_id_resolver ? m_type_id_resolver(hash) : std::nullopt;
            record(
                    [&](auto &m)
                    {
                        m.record_sample(hash, info, id.value_or(0), id.has_value(), fidelity,
                                        bytes);
                    });
        }

        // Encode the edge into whichever discipline the variant holds, then re-arm the
        // cooperative drain (event-driven resume — the quiescent drain wakes on the push).
        template<typename Fn>
        void record(Fn &&fn)
        {
            std::visit(std::forward<Fn>(fn), m_block.machinery);
            m_block.kick();
        }

        block                                                                  &m_block;
        const io::message_info                                                  m_empty_info{};
        plexus::detail::move_only_function<io::capture_fidelity(std::uint64_t)> m_fidelity_resolver;
        type_id_resolver                                                        m_type_id_resolver;
    };

    Engine                        *m_engine;
    typename Engine::executor_type m_executor;
    std::shared_ptr<block>         m_block;
    std::unique_ptr<io::observer>  m_sink;
};

}

#endif
