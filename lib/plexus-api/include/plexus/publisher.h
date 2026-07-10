#ifndef HPP_GUARD_PLEXUS_PUBLISHER_H
#define HPP_GUARD_PLEXUS_PUBLISHER_H

#include "plexus/node.h"

#include "plexus/io/object_carrier.h"
#include "plexus/io/endpoint_seam.h"

#include "plexus/detail/loan_pool.h"
#include "plexus/detail/publisher_publish.h"

#include "plexus/typed_codec.h"
#include "plexus/topic_qos.h"
#include "plexus/wire_bytes.h"
#include "plexus/recording_qos.h"
#include "plexus/typed_publisher_options.h"

#include "plexus/detail/compat.h"

#include <span>
#include <string>
#include <cstddef>
#include <cstdint>
#include <utility>
#include <optional>
#include <string_view>

namespace plexus {

template<typename Codec>
class publisher;

// LIFETIME: a publisher must NOT outlive its node (member-init aggregation, node ref first, so
// reverse destruction retires the handle before the node). A moved-from handle is inert.
template<>
class publisher<void>
{
public:
    template<typename Policy, typename... NodeTs>
    publisher(node<Policy, NodeTs...> &n, std::string_view fqn, const topic_qos &qos = {}, bool emit_source_identity = false, const void *geometry = nullptr)
            : m_seam(n.endpoint_seam_for())
            , m_fqn(fqn)
    {
        m_seam.declare_publisher(m_seam.ctx, fqn, qos, emit_source_identity, std::nullopt, {}, 0, geometry, std::nullopt);
    }

    void publish(std::span<const std::byte> bytes)
    {
        m_seam.publish(m_seam.ctx, m_fqn, bytes);
    }

    publisher(publisher &&other) noexcept
            : m_seam(std::exchange(other.m_seam, io::endpoint_seam{}))
            , m_fqn(std::move(other.m_fqn))
    {
    }

    publisher(const publisher &)            = delete;
    publisher &operator=(const publisher &) = delete;
    publisher &operator=(publisher &&)      = delete;

    ~publisher() noexcept
    {
        detail::retire_publisher_quiet(m_seam, m_fqn);
    }

private:
    io::endpoint_seam m_seam{};
    std::string m_fqn;
};

// The ctor declares the topic with its resolved type identity (the gate a strict subscriber
// matches) and owns a fixed loan pool. On pool exhaustion publish takes the serialize path and
// bumps loan_exhausted() — never blocks, never allocates. Lifetime as publisher<void>.
template<typename Codec>
class publisher
{
public:
    using value_type = typename Codec::value_type;

    template<typename Policy, typename... NodeTs>
    publisher(node<Policy, NodeTs...> &n, std::string_view fqn, const typed_publisher_options &opts = {}, Codec codec = {})
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
        m_seam.declare_publisher(m_seam.ctx, fqn, opts.qos, opts.emit_source_identity, m_identity.type_id, m_identity.type_name, m_identity.schema_hint, opts.geometry,
                                 opts.capture ? std::optional{opts.capture->to_rule()} : std::nullopt);
    }

    // Borrow a slot to construct a value in place (zero-copy publish); an empty loan signals
    // pool exhaustion.
    template<typename... Args>
    detail::loan<value_type> borrow(Args &&...args)
    {
        return m_pool.try_borrow(std::forward<Args>(args)...);
    }

    void publish(detail::loan<value_type> &&held)
    {
        detail::publish_loan(*this, std::move(held));
    }
    void publish(const value_type &value)
    {
        detail::publish_value(*this, value);
    }

    // Publishes that degraded to the serialize path.
    std::size_t loan_exhausted() const noexcept
    {
        return m_loan_exhausted;
    }

    publisher(publisher &&other) noexcept
            : m_seam(std::exchange(other.m_seam, io::endpoint_seam{}))
            , m_fqn(std::move(other.m_fqn))
            , m_codec(std::move(other.m_codec))
            , m_identity(other.m_identity)
            , m_pool(std::move(other.m_pool))
            , m_loan_exhausted(other.m_loan_exhausted)
    {
    }

    publisher(const publisher &)            = delete;
    publisher &operator=(const publisher &) = delete;
    publisher &operator=(publisher &&)      = delete;

    ~publisher() noexcept
    {
        detail::retire_publisher_quiet(m_seam, m_fqn);
    }

private:
    template<typename P, typename V>
    friend std::span<const std::byte> detail::encode_to_span(P &, const V &);
    template<typename P, typename V>
    friend void detail::publish_loan(P &, detail::loan<V> &&);
    template<typename P, typename V>
    friend void detail::publish_value(P &, const V &);

    io::endpoint_seam m_seam{};
    std::string m_fqn;
    Codec m_codec;
    type_identity m_identity;
    detail::loan_pool<value_type> m_pool;
    wire_bytes<> m_scratch;
    std::size_t m_loan_exhausted{};
};

}

#endif
