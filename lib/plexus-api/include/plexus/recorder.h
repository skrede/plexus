#ifndef HPP_GUARD_PLEXUS_API_RECORDER_H
#define HPP_GUARD_PLEXUS_API_RECORDER_H

#include "plexus/recorder_options.h"
#include "plexus/wire_capture_qos.h"
#include "plexus/detail/recorder_drain.h"

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

// over-limit: one cohesive RAII recording handle; the public attach/trigger/flush/pump verbs +
// the mode-machinery accessors + the schema/preamble builders + the cooperative drain post all
// drive the shared m_block (the stable-address drain block) under one teardown discipline, so
// splitting the surface scatters that shared recording state (the drain block + observer-adapter
// sink are extracted to detail/recorder_drain.h).
//
// The RAII recorder handle returned by node.make_recorder (a sibling of the
// publisher/subscriber/procedure endpoint family). It owns the recording machinery (continuous
// flat_recorder or pre_buffer FDR controller over one ring) and the observer-adapter sink that
// feeds it. On construction it registers the sink (add_observer auto-feeds the capture gate's
// observer-presence floor) and, in continuous mode, emits the stream header + Definitions preamble.
//
// LIFETIME (load-bearing): the dtor DEREGISTERS the sink before any engine/executor teardown so no
// posted drain closure outlives the ring it reads; it drains the ring synchronously, then drops the
// heap drain block (an in-flight task holding a weak handle finds it expired and stops). Move-only
// (single-owner, no shared_from_this); the heap block keeps the ring address stable across a move.
template<typename Engine, typename Policy>
class recorder
{
    // The cooperative drain block + the observer-adapter sink are relocated to
    // detail/recorder_drain.h (they carry no Engine dependency); the recorder owns one block by
    // shared_ptr (stable heap address across a move) and one sink that feeds it.
    using block = detail::recorder_block;

public:
    using anomaly_predicate = recorder_options::anomaly_predicate;

    // Constructed only by node.make_recorder. The engine/executor/byte_sink are borrowed and MUST
    // outlive the recorder. The crypto position records which wire-capture tap the stream used.
    recorder(Engine &engine, typename Engine::executor_type executor, const node_id &id, io::recording::byte_sink &sink, recorder_options opts, wire_crypto_position crypto)
            : m_engine(&engine)
            , m_executor(executor)
            , m_block(std::make_shared<block>(sink, opts, io::recording::flat_recorder::clock_fn{[] { return wire::now_timestamp_ns(); }}, preamble_scratch_bytes(opts.schemas)))
            , m_sink(make_sink())
    {
        if(opts.mode == recording_mode::pre_buffer && opts.on_anomaly)
            pre().on_anomaly(std::move(*opts.on_anomaly));
        if(opts.mode == recording_mode::continuous)
        {
            const auto rows = schema_rows(opts.schemas);
            // The on-disk ordinals of capture_crypto_position are pinned to the public
            // wire_crypto_position (cleartext=0, ciphertext=1), so the cast is a forward.
            flat().open(id, m_engine->capture().default_rule(), rows, static_cast<io::recording::capture_crypto_position>(crypto));
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

    // The teardown discipline: deregister the sink FIRST, drain the ring out to the sink, then drop
    // the drain block so an already-posted task finds it expired and exits without touching freed
    // state. A dtor must not throw — every step is non-throwing or wrapped.
    ~recorder()
    {
        if(m_engine == nullptr)
            return;
#if defined(__cpp_exceptions)
        try
        {
            drain_to_sink();
        }
        catch(...)
        {
        }
#else
        drain_to_sink();
#endif
    }

    // The FDR surface (pre_buffer mode): trigger() freezes the held window (alloc-free); in
    // continuous mode it is a no-op and the span is 0.
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

    // Drive one bounded cooperative drain batch manually; returns true while work remains (the
    // self-re-posting task calls this on the consumer's turns).
    bool pump()
    {
        return m_block->pump();
    }

    void flush()
    {
        std::visit([](auto &m) { m.flush(); }, m_block->machinery);
    }

private:
    // The teardown drain step: deregister the sink FIRST, then drain the ring out to it and flush.
    // Called from the dtor, wrapped under exceptions and run bare under -fno-exceptions.
    void drain_to_sink()
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

    io::recording::flat_recorder &flat()
    {
        return std::get<io::recording::flat_recorder>(m_block->machinery);
    }
    io::recording::pre_buffer_controller &pre()
    {
        return std::get<io::recording::pre_buffer_controller>(m_block->machinery);
    }

    // Build the observer-adapter sink with the topic-hash -> fidelity and -> producer-type_id
    // resolvers it stamps each sample with (so an offline projector keys it to the declared
    // schema).
    std::unique_ptr<io::observer> make_sink()
    {
        return std::make_unique<detail::recorder_variant_sink>(
                *m_block, [engine = m_engine](std::uint64_t hash) { return engine->capture().rule_for(hash).fidelity; },
                [engine = m_engine](std::uint64_t hash) { return engine->messages().producer_type_id(hash); });
    }

    // Translate the public schemas into the core preamble rows (field-for-field; open() copies the
    // bytes into the stream synchronously, so the views may alias opts.schemas).
    static std::vector<io::recording::type_schema_entry> schema_rows(const std::vector<type_schema> &schemas)
    {
        std::vector<io::recording::type_schema_entry> rows;
        rows.reserve(schemas.size());
        for(const type_schema &s : schemas)
            rows.push_back({s.type_id, {}, s.message_encoding, s.schema_name, s.schema_encoding, s.schema_data});
        return rows;
    }

    // The worst-case Definitions-preamble byte size — used ONCE to size the writer's
    // bounds-check-free scratch so a multi-KiB schema blob cannot overrun it (wire::writer is a raw
    // memcpy). Sums each entry's worst-case varint-prefixed blobs plus a header margin.
    static std::size_t preamble_scratch_bytes(const std::vector<type_schema> &schemas)
    {
        constexpr std::size_t k_default_scratch = 64u * 1024u;
        constexpr std::size_t k_header_margin   = 256u;
        auto                  blob              = [](std::size_t n) { return wire::varint_size(n) + n; };
        std::size_t           need              = k_header_margin + wire::varint_size(schemas.size());
        for(const type_schema &s : schemas)
            need += wire::varint_size(s.type_id) + blob(0) + blob(s.message_encoding.size()) + blob(s.schema_name.size()) + blob(s.schema_encoding.size()) + blob(s.schema_data.size());
        return std::max(k_default_scratch, need);
    }

    // Post one cooperative drain turn. It re-posts ITSELF only while the ring holds records; when
    // empty it clears `armed` and goes quiescent (the next edge re-arms it through block::kick), so
    // it never starves a cooperative executor. It holds a weak handle, so once the recorder is gone
    // the lock fails and the task exits, never reading a freed ring (the teardown-UAF discipline).
    // The executor is captured as a plain pointer so the closure stays trivially copyable (the
    // reference-typed executor would otherwise be copied).
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

    Engine                        *m_engine;
    typename Engine::executor_type m_executor;
    std::shared_ptr<block>         m_block;
    std::unique_ptr<io::observer>  m_sink;
};

}

#endif
