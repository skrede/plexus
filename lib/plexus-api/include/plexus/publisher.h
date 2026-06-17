#ifndef HPP_GUARD_PLEXUS_PUBLISHER_H
#define HPP_GUARD_PLEXUS_PUBLISHER_H

#include "plexus/node.h"

#include "plexus/io/object_carrier.h"
#include "plexus/io/endpoint_seam.h"

#include "plexus/detail/loan_pool.h"

#include "plexus/typed_codec.h"
#include "plexus/topic_qos.h"
#include "plexus/wire_bytes.h"

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
// (publisher<Codec> below); the bytes endpoint is the publisher<void> specialization —
// publisher<> selects it via the defaulted Codec (the default lives in node.h's forward
// declaration, seen first), so every bytes spelling keeps compiling unchanged.
template <typename Codec>
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

    // The producer-side same-host ring geometry override. std::optional because ABSENCE
    // is meaningful: unset falls back to the node-level shm_geometry default, present
    // overrides per topic — "not declared" is distinct from "declared as the default".
    // It is producer-side same-host provisioning only, never wire-advertised, never RxO.
    std::optional<io::shm::shm_geometry> shm_geometry{};
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
template <>
class publisher<void>
{
public:
    // The registration ctor: declare the topic (minting its gid) through the node's
    // private friend seam, then capture the type-erased outbound seam for the direct
    // publish. The ctor stays a template over <Policy, NodeTs...> so it binds across the
    // node's transport pack; the captured seam carries no Policy.
    template <typename Policy, typename... NodeTs>
    publisher(node<Policy, NodeTs...> &n, std::string_view fqn, const topic_qos &qos = {},
              bool emit_source_identity = false,
              std::optional<io::shm::shm_geometry> shm_geometry = std::nullopt)
        : m_seam(n.endpoint_seam_for())
        , m_fqn(fqn)
    {
        m_seam.declare_publisher(m_seam.ctx, fqn, qos, emit_source_identity, std::nullopt,
                                 shm_geometry);
    }

    // The hot verb: one indirect call across the seam. A moved-from publisher has a null
    // seam ctx and must not be published through (the lifetime precondition); the node
    // sequences teardown.
    void publish(std::span<const std::byte> bytes) { m_seam.publish(m_seam.ctx, m_fqn, bytes); }

    publisher(publisher &&other) noexcept
        : m_seam(std::exchange(other.m_seam, io::endpoint_seam{}))
        , m_fqn(std::move(other.m_fqn))
    {
    }

    publisher &operator=(publisher &&other) noexcept
    {
        if(this != &other)
        {
            m_seam = std::exchange(other.m_seam, io::endpoint_seam{});
            m_fqn = std::move(other.m_fqn);
        }
        return *this;
    }

    publisher(const publisher &) = delete;
    publisher &operator=(const publisher &) = delete;

    // The producer DECLARATION persists for the node's life so the endpoint identity stays
    // stable for subscriber correlation (declaration lifetime); the HANDLE's lifetime is a
    // distinct concept — its drop posts a handle-lifetime edge through retire_publisher.
    // The dtor gates on the moved-from sentinel (m_seam.ctx == nullptr) ALONE; the seam
    // ALWAYS wires retire_publisher at construction, so there is no defensive null check on
    // it. A dtor must not throw, so the posted edge is wrapped to swallow any exception (a
    // throwing dtor is UB) — the shared posted-edge-from-dtor safety pattern, also used by
    // ~node.
    ~publisher() noexcept
    {
        if(m_seam.ctx != nullptr)
            try { m_seam.retire_publisher(m_seam.ctx, m_fqn); } catch(...) {}
    }

private:
    io::endpoint_seam m_seam{};
    std::string       m_fqn;
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
template <typename Codec>
class publisher
{
public:
    using value_type = typename Codec::value_type;

    template <typename Policy, typename... NodeTs>
    publisher(node<Policy, NodeTs...> &n, std::string_view fqn,
              const typed_publisher_options &opts = {}, Codec codec = {})
        : m_seam(n.endpoint_seam_for())
        , m_fqn(fqn)
        , m_codec(std::move(codec))
        , m_identity(resolve_identity(m_codec, opts.type_id))
        , m_pool(opts.pool_depth)
    {
        static_assert(typed_codec<Codec>,
                      "plexus: a typed publisher needs a codec satisfying typed_codec "
                      "(value_type; encode(const value_type&) -> wire_bytes<>; "
                      "decode(span, value_type&) -> expected<void, error_code>).");
        m_seam.declare_publisher(m_seam.ctx, fqn, opts.qos, opts.emit_source_identity,
                                 m_identity.type_id, opts.shm_geometry);
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
        auto encode = [&] { return encode_to_span(value); };
        m_seam.publish_object(m_seam.ctx, m_fqn, carrier, io::make_encode_thunk(encode));
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
        m_seam.publish(m_seam.ctx, m_fqn, encode_to_span(value));
    }

    // The count of publishes that degraded to the serialize path because the pool had no
    // free slot — the readable exhaustion signal (never a silent stall).
    [[nodiscard]] std::size_t loan_exhausted() const noexcept { return m_loan_exhausted; }

    publisher(publisher &&other) noexcept
        : m_seam(std::exchange(other.m_seam, io::endpoint_seam{}))
        , m_fqn(std::move(other.m_fqn))
        , m_codec(std::move(other.m_codec))
        , m_identity(other.m_identity)
        , m_pool(std::move(other.m_pool))
        , m_loan_exhausted(other.m_loan_exhausted)
    {
    }

    publisher(const publisher &) = delete;
    publisher &operator=(const publisher &) = delete;
    publisher &operator=(publisher &&) = delete;

    // Handle-lifetime drop edge via the same posted-edge-from-dtor safety pattern as the
    // bytes publisher<void>: gate on the moved-from sentinel alone, swallow any exception.
    ~publisher() noexcept
    {
        if(m_seam.ctx != nullptr)
            try { m_seam.retire_publisher(m_seam.ctx, m_fqn); } catch(...) {}
    }

private:
    std::span<const std::byte> encode_to_span(const value_type &value)
    {
        m_scratch = m_codec.encode(value);
        return static_cast<std::span<const std::byte>>(m_scratch);
    }

    io::endpoint_seam                  m_seam{};
    std::string                        m_fqn;
    Codec                              m_codec;
    type_identity                      m_identity;
    detail::loan_pool<value_type>      m_pool;
    wire_bytes<>                       m_scratch;
    std::size_t                        m_loan_exhausted{};
};

}

#endif
