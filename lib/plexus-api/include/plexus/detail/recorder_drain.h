#ifndef HPP_GUARD_PLEXUS_DETAIL_RECORDER_DRAIN_H
#define HPP_GUARD_PLEXUS_DETAIL_RECORDER_DRAIN_H

#include "plexus/recorder_options.h"

#include "plexus/io/observer.h"
#include "plexus/io/message_info.h"
#include "plexus/io/recording/flat_recorder.h"
#include "plexus/io/recording/pre_buffer_controller.h"

#include "plexus/wire/topic_hash.h"

#include "plexus/detail/compat.h"

#include <span>
#include <string>
#include <variant>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <utility>
#include <string_view>

namespace plexus::detail {

// The recorder's cooperative drain block, relocated out of the recorder template (it carries no
// Engine dependency). The recording state lives behind a stable heap address so a posted drain
// task captured once at attach survives a move of the owning handle. The drain is EVENT-DRIVEN
// re-arm: a posted task drains a bounded batch and re-posts ITSELF only while the ring still holds
// records; once empty it goes quiescent and the next pushed edge re-arms it (kick()).
struct recorder_block
{
    using mode_variant =
            std::variant<io::recording::flat_recorder, io::recording::pre_buffer_controller>;

    recorder_block(io::recording::byte_sink &sink, const recorder_options &opts,
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

    bool pump() { return std::visit([](auto &m) { return m.pump(); }, machinery); }

    // Re-arm the cooperative drain if it is quiescent — called from the observer turn after a
    // push, the event-driven resume.
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

// A small observer that dispatches every posted edge to the variant-held recorder. It mirrors
// recording_sink's edge set over the std::variant (the two disciplines share the record_*
// contract, visited without virtual indirection on the build turn).
class recorder_variant_sink : public io::observer
{
public:
    using type_id_resolver =
            plexus::detail::move_only_function<std::optional<std::uint64_t>(std::uint64_t)>;

    // The fidelity resolver maps a topic hash to its capture_policy-resolved tier so a recorded
    // sample is stamped with its true fidelity (unset falls back to payload); the type_id resolver
    // maps it to its registered producer type_id so the sample keys to the declared schema.
    recorder_variant_sink(
            recorder_block                                                         &blk,
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
        record([&](auto &m)
               { m.record_sample(hash, info, id.value_or(0), id.has_value(), fidelity, bytes); });
    }

    // Encode the edge into whichever discipline the variant holds, then re-arm the cooperative
    // drain (event-driven resume — the quiescent drain wakes on the push).
    template<typename Fn>
    void record(Fn &&fn)
    {
        std::visit(std::forward<Fn>(fn), m_block.machinery);
        m_block.kick();
    }

    recorder_block                                                         &m_block;
    const io::message_info                                                  m_empty_info{};
    plexus::detail::move_only_function<io::capture_fidelity(std::uint64_t)> m_fidelity_resolver;
    type_id_resolver                                                        m_type_id_resolver;
};

}

#endif
