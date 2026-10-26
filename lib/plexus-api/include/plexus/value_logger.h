#ifndef HPP_GUARD_PLEXUS_VALUE_LOGGER_H
#define HPP_GUARD_PLEXUS_VALUE_LOGGER_H

#include "plexus/node.h"

#include "plexus/io/message_info.h"
#include "plexus/io/object_carrier.h"
#include "plexus/io/subscriber_qos.h"
#include "plexus/io/endpoint_seam.h"

#include "plexus/typed_codec.h"
#include "plexus/value_projection.h"
#include "plexus/value_logger_options.h"
#include "plexus/detail/value_logger_project.h"

#include <span>
#include <memory>
#include <string>
#include <cstddef>
#include <cstdint>
#include <ostream>
#include <utility>
#include <string_view>
#include <type_traits>

namespace plexus {

// An empty tag selecting the streamable text floor: it satisfies no value_projection, so a
// value_logger with no supplied projection formats through the operator<< floor.
struct no_projection
{
};

// A subscriber<Codec> whose success branch formats one decoded value to a printable record on a
// consumer-owned ostream rather than invoking a user callback. It registers BOTH a bytes leg
// (decode->format) and a process-tier object-dispatch leg (recover the concrete T from the
// carrier, format with no decode) under ONE registration id.
//
// LIFETIME: a value_logger must NOT outlive its node nor the ostream it writes to (both are
// borrowed). The decode + format state lives in a heap block the handle owns; the node's stored
// adapters reference it and are retired before the block is freed.
template<typename Codec, typename Projection>
class value_logger
{
public:
    using value_type = typename Codec::value_type;

    static_assert(typed_codec<Codec>,
                  "plexus: a value_logger needs a codec satisfying typed_codec "
                  "(value_type; encode(const value_type&) -> wire_bytes<>; "
                  "decode(span, value_type&) -> expected<void, error_code>).");
    static_assert(identity_bearing<Codec>,
                  "plexus: a typed endpoint needs a type identity — give the value_logger's codec a "
                  "type_info() -> type_identity member, or wrap it in plexus::with_identity.");
    static_assert(loggable_value<Projection, value_type>,
                  "plexus: a value_logger needs the value type to either supply a "
                  "value_projection (column names + emit_fields/emit_json) or be "
                  "streamable to an ostream (the operator<< text floor).");

    template<typename Policy, typename... NodeTs>
    value_logger(node<Policy, NodeTs...> &n, std::string_view fqn, const value_logger_options &opts, Codec codec = {}, Projection projection = {})
            : m_seam(n.endpoint_seam_for())
            , m_state(std::make_unique<state>(std::move(codec), std::move(projection), opts.format, opts.out))
            , m_rid(enroll(m_seam, fqn, opts, *m_state))
    {
    }

    // Inbound frames whose decode failed — dropped, never a partial line.
    std::size_t decode_failed() const noexcept
    {
        m_seam.affine(m_seam.ctx, "decode_failed");
        return m_state->decode_failed;
    }

    value_logger(value_logger &&other) noexcept
            : m_seam(std::exchange(other.m_seam, io::endpoint_seam{}))
            , m_state(std::move(other.m_state))
            , m_rid(other.m_rid)
    {
    }

    value_logger(const value_logger &)            = delete;
    value_logger &operator=(const value_logger &) = delete;
    value_logger &operator=(value_logger &&)      = delete;

    ~value_logger()
    {
        if(m_seam.ctx != nullptr)
            m_seam.retire_subscriber(m_seam.ctx, m_rid);
    }

private:
    static constexpr bool has_projection = value_projection<Projection, value_type>;

    // The slot and buffer are cleared and reused per record so the steady loop allocates nothing.
    struct state
    {
        Codec codec;
        Projection projection;
        log_format format;
        std::ostream &out;
        value_type slot;
        std::string buffer;
        std::size_t decode_failed;
        bool header_written;

        state(Codec c, Projection p, log_format fmt, std::ostream &o)
                : codec(std::move(c))
                , projection(std::move(p))
                , format(fmt)
                , out(o)
                , slot{}
                , decode_failed{}
                , header_written{}
        {
        }

        // Success formats the record into the reused buffer and writes it; failure increments the
        // counter and drops — never a partial line.
        void on_bytes(std::span<const std::byte> bytes, const io::message_info &info)
        {
            auto decoded = codec.decode(bytes, slot);
            if(!decoded.has_value())
            {
                ++decode_failed;
                return;
            }
            detail::vl_format_record<has_projection>(*this, info);
            out << buffer;
        }

        // Recover the concrete T from the carrier (no decode) and format it through the SAME
        // projection path. A view-type value_type aliases the carrier slot for this call only.
        void on_object(const io::object_carrier &carrier, const io::message_info &info)
        {
            slot = *static_cast<const value_type *>(carrier.slot->object);
            detail::vl_format_record<has_projection>(*this, info);
            out << buffer;
        }
    };

    io::endpoint_seam m_seam;
    std::unique_ptr<state> m_state;
    std::uint64_t m_rid;

    static std::uint64_t enroll(const io::endpoint_seam &seam, std::string_view fqn, const value_logger_options &opts, state &st)
    {
        const auto identity    = resolve_identity(st.codec, opts.type_id);
        io::subscriber_qos qos = opts.qos;
        qos.posture            = opts.posture;
        qos.wants_message_info = true;

        auto bytes_adapter           = [&st](std::span<const std::byte> bytes, const io::message_info &info) { st.on_bytes(bytes, info); };
        io::object_dispatch dispatch = [&st](const io::object_carrier &carrier, const io::message_info &info) { st.on_object(carrier, info); };
        return seam.register_subscriber(seam.ctx, fqn, qos, std::move(bytes_adapter), identity.type_id, identity.type_name, &io::detail::type_key<value_type>, std::move(dispatch),
                                        std::nullopt);
    }
};

}

#endif
