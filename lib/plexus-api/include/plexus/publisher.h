#ifndef HPP_GUARD_PLEXUS_PUBLISHER_H
#define HPP_GUARD_PLEXUS_PUBLISHER_H

#include "plexus/node.h"

#include "plexus/io/message_forwarder.h"
#include "plexus/io/object_carrier.h"

#include "plexus/detail/loan_pool.h"

#include "plexus/typed_codec.h"
#include "plexus/topic_qos.h"
#include "plexus/wire_bytes.h"
#include "plexus/policy.h"

#include "plexus/detail/compat.h"

#include <span>
#include <string>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>
#include <string_view>

namespace plexus {

// The publishing endpoint family. The primary template names the typed endpoint
// (publisher<Policy, Codec> below); the bytes endpoint is the publisher<Policy, void>
// specialization — publisher<Policy> selects it via the defaulted Codec, so every
// existing bytes spelling keeps compiling unchanged.
template <typename Policy, typename Codec>
    requires plexus::Policy<Policy>
class publisher;

// The typed publisher's construction-time options. The pool depth is required-with-
// default: a consumer that states nothing genuinely chose 8 (a provisional geometry the
// perf rig substantiates), and an explicit type identity overrides the codec's own
// (resolve_identity's explicit-wins precedence). emit_source_identity mirrors the bytes
// publisher's flag.
struct typed_publisher_options
{
    topic_qos                     qos{};
    bool                          emit_source_identity = false;
    std::size_t                   pool_depth           = 8;
    std::optional<type_identity>  type_id{};
};

// The bytes publishing endpoint: the CONSTRUCTOR is the registration — it declares the
// topic on the node and mints the endpoint gid — and the handle owns the publish verb.
// publish is a DIRECT (non-virtual) forwarder call — the hot path carries no type
// erasure; only the cold retire is erased.
//
// LIFETIME: a publisher must NOT outlive its node. The canonical usage is
// member-init aggregation — declare the node ref first and the endpoint handles after
// it, so reverse destruction retires the handles before the node. A moved-from handle
// is inert (null forwarder, empty retire); its destructor does nothing.
template <typename Policy>
    requires plexus::Policy<Policy>
class publisher<Policy, void>
{
public:
    // The registration ctor: declare the topic (minting its gid) through the node's
    // private friend seam, then capture the forwarder for the direct publish. Spelled
    // over node<Policy, NodeTs...> so it binds across the node's transport pack.
    template <typename... NodeTs>
    publisher(node<Policy, NodeTs...> &n, std::string_view fqn, const topic_qos &qos = {},
              bool emit_source_identity = false)
        : m_messages(&n.message_forwarder())
        , m_fqn(fqn)
    {
        n.declare_publisher_seam(fqn, qos, emit_source_identity);
    }

    // The hot verb: a direct fan of bytes through the owned forwarder. A moved-from
    // publisher has a null forwarder and must not be published through (the lifetime
    // precondition); the node sequences teardown.
    void publish(std::span<const std::byte> bytes) { m_messages->publish(m_fqn, bytes); }

    publisher(publisher &&other) noexcept
        : m_messages(std::exchange(other.m_messages, nullptr))
        , m_fqn(std::move(other.m_fqn))
    {
    }

    publisher &operator=(publisher &&other) noexcept
    {
        if(this != &other)
        {
            m_messages = std::exchange(other.m_messages, nullptr);
            m_fqn = std::move(other.m_fqn);
        }
        return *this;
    }

    publisher(const publisher &) = delete;
    publisher &operator=(const publisher &) = delete;

    // The declaration persists for the node's life so the endpoint counter stays stable
    // and is never reused: a dropped publisher simply stops publishing. There
    // is no per-publisher resource to reclaim, so the destructor is trivial.
    ~publisher() = default;

private:
    io::message_forwarder<Policy> *m_messages{};
    std::string                    m_fqn;
};

// The typed publishing endpoint: the CONSTRUCTOR declares the topic WITH its resolved
// type identity (the gate a strict subscriber matches) and owns a fixed loan pool. Two
// publish forms reach the SAME dual-path forwarder verb:
//   - publish(loan<T>&&) is true zero-copy: the object the consumer built in place is
//     handed to the carrier and rides the process-tier lane by address; encode is never
//     invoked for an in-process subscriber.
//   - publish(const T&) copies one T into a borrowed slot then delegates (one copy, zero
//     serialization in process).
// On pool exhaustion BOTH forms take the pure serialize path — encode immediately and
// publish the bytes — and bump loan_exhausted(); this never blocks and never allocates.
//
// LIFETIME: a publisher must NOT outlive its node (member-init aggregation, node ref
// first). A moved-from handle is inert (null forwarder); its destructor does nothing.
template <typename Policy, typename Codec>
    requires plexus::Policy<Policy>
class publisher
{
public:
    using value_type = typename Codec::value_type;

    template <typename... NodeTs>
    publisher(node<Policy, NodeTs...> &n, std::string_view fqn,
              const typed_publisher_options &opts = {}, Codec codec = {})
        : m_messages(&n.message_forwarder())
        , m_fqn(fqn)
        , m_codec(std::move(codec))
        , m_identity(resolve_identity(m_codec, opts.type_id))
        , m_pool(opts.pool_depth)
    {
        static_assert(typed_codec<Codec>,
                      "plexus: a typed publisher needs a codec satisfying typed_codec "
                      "(value_type; encode(const value_type&) -> wire_bytes<>; "
                      "decode(span, value_type&) -> expected<void, error_code>).");
        n.declare_publisher_seam(fqn, opts.qos, opts.emit_source_identity, m_identity.type_id);
    }

    // Borrow a slot to construct a value in place (zero-copy publish). An empty loan on
    // pool exhaustion — publish(loan&&) of an empty loan takes the serialize path on a
    // freshly encoded value the consumer supplies, so the caller checks valid() only when
    // it wants to react to exhaustion itself.
    template <typename... Args>
    detail::loan<value_type> borrow(Args &&...args)
    {
        return m_pool.try_borrow(std::forward<Args>(args)...);
    }

    // Publish a borrowed object zero-copy. A valid loan rides the fast path by address;
    // an empty loan (the consumer published an exhausted borrow) is a no-op — the consumer
    // owns nothing to send. The encode lambda holds its wire_bytes for the synchronous
    // verb's duration (the egress copies before the verb returns).
    void publish(detail::loan<value_type> &&held)
    {
        if(!held)
            return;
        auto carrier = detail::loan_pool<value_type>::carrier_for(held, m_identity.type_id);
        const value_type &value = *static_cast<const value_type *>(carrier.slot->object);
        m_messages->publish_object(m_fqn, carrier, [&] { return encode_to_span(value); });
    }

    // The convenience copy form: borrow a slot, copy-construct into it, and delegate to
    // the zero-copy path. On exhaustion, serialize the value directly and publish bytes —
    // a counted graceful degradation that never blocks or allocates.
    void publish(const value_type &value)
    {
        auto held = m_pool.try_borrow(value);
        if(held)
        {
            publish(std::move(held));
            return;
        }
        ++m_loan_exhausted;
        m_messages->publish(m_fqn, encode_to_span(value));
    }

    // The count of publishes that degraded to the serialize path because the pool had no
    // free slot — the readable exhaustion signal (never a silent stall).
    [[nodiscard]] std::size_t loan_exhausted() const noexcept { return m_loan_exhausted; }

    publisher(publisher &&other) noexcept
        : m_messages(std::exchange(other.m_messages, nullptr))
        , m_fqn(std::move(other.m_fqn))
        , m_codec(std::move(other.m_codec))
        , m_identity(other.m_identity)
        , m_pool(other.m_pool.capacity())
        , m_loan_exhausted(other.m_loan_exhausted)
    {
    }

    publisher(const publisher &) = delete;
    publisher &operator=(const publisher &) = delete;
    publisher &operator=(publisher &&) = delete;

    ~publisher() = default;

private:
    std::span<const std::byte> encode_to_span(const value_type &value)
    {
        m_scratch = m_codec.encode(value);
        return static_cast<std::span<const std::byte>>(m_scratch);
    }

    io::message_forwarder<Policy>     *m_messages{};
    std::string                        m_fqn;
    Codec                              m_codec;
    type_identity                      m_identity;
    detail::loan_pool<value_type>      m_pool;
    wire_bytes<>                       m_scratch;
    std::size_t                        m_loan_exhausted{};
};

}

#endif
