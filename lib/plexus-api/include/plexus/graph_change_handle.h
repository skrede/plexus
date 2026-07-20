#ifndef HPP_GUARD_PLEXUS_GRAPH_CHANGE_HANDLE_H
#define HPP_GUARD_PLEXUS_GRAPH_CHANGE_HANDLE_H

#include "plexus/io/observer.h"

#include "plexus/graph/graph_change.h"

#include "plexus/detail/compat.h"

#include <memory>
#include <cstdint>
#include <utility>

namespace plexus {

// A move-only RAII registration over the engine's observer seam: it owns a small observer sink
// carrying the consumer's coarse on_graph_changed(uint64) callback (and, on a host profile, the
// {who, kind} delta callback), added via add_observer at construction and removed in the dtor. The
// handle takes the teardown ordering off the consumer (D-03): the sink leaves the live observer list
// before its owner is gone, so a coalesced wakeup never fires into freed state. No drain block is
// needed — the engine owns the coalescer, so this side has nothing to flush and the dtor only
// deregisters.
class graph_change_handle
{
    // add_observer stores a reference_wrapper<observer>, so the sink must keep a stable address across
    // a handle move: it lives in a heap block owned by unique_ptr, and a move transfers the pointer,
    // not the object.
    struct sink : io::observer
    {
        sink(plexus::detail::move_only_function<void(std::uint64_t)> on_changed,
             plexus::detail::move_only_function<void(const graph::graph_change &)> on_delta)
                : m_on_changed(std::move(on_changed))
                , m_on_delta(std::move(on_delta))
        {
        }

        void on_graph_changed(std::uint64_t generation) override
        {
            if(m_on_changed != nullptr)
                m_on_changed(generation);
        }

        void on_graph_delta(const graph::graph_change &change) override
        {
            if(m_on_delta != nullptr)
                m_on_delta(change);
        }

        // The engine gates the host edge-log on this property, so it is engaged only when the consumer
        // supplied a delta callback: a coarse-only handle leaves the log inert.
        bool observes_graph() const override
        {
            return m_on_delta != nullptr;
        }

        plexus::detail::move_only_function<void(std::uint64_t)> m_on_changed;
        plexus::detail::move_only_function<void(const graph::graph_change &)> m_on_delta;
    };

public:
    template<typename Engine>
    graph_change_handle(Engine &engine, plexus::detail::move_only_function<void(std::uint64_t)> on_changed,
                        plexus::detail::move_only_function<void(const graph::graph_change &)> on_delta = {})
            : m_sink(std::make_unique<sink>(std::move(on_changed), std::move(on_delta)))
    {
        engine.add_observer(*m_sink);
        m_retire = [engine_ptr = &engine, sink_ptr = m_sink.get()] { engine_ptr->remove_observer(*sink_ptr); };
    }

    graph_change_handle(graph_change_handle &&) noexcept        = default;

    graph_change_handle(const graph_change_handle &)            = delete;
    graph_change_handle &operator=(const graph_change_handle &) = delete;
    graph_change_handle &operator=(graph_change_handle &&)      = delete;

    ~graph_change_handle()
    {
        if(m_retire != nullptr)
            m_retire();
    }

private:
    std::unique_ptr<sink> m_sink;
    plexus::detail::move_only_function<void()> m_retire;
};

}

#endif
