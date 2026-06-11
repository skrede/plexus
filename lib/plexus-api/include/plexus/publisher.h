#ifndef HPP_GUARD_PLEXUS_PUBLISHER_H
#define HPP_GUARD_PLEXUS_PUBLISHER_H

#include "plexus/node.h"

#include "plexus/io/message_forwarder.h"

#include "plexus/topic_qos.h"
#include "plexus/policy.h"

#include "plexus/detail/compat.h"

#include <span>
#include <string>
#include <cstddef>
#include <utility>
#include <string_view>

namespace plexus {

// A move-only RAII publishing endpoint (D-10/D-11): the CONSTRUCTOR is the
// registration — it declares the topic on the node and mints the endpoint gid
// (API-04) — and the handle owns the publish verb. Templated on Policy ALONE (both
// forwarders are Policy-only), so one publisher type serves a node over any transport
// pack. publish is a DIRECT (non-virtual) forwarder call — the hot path carries no
// type erasure; only the cold retire is erased.
//
// LIFETIME (D-13): a publisher must NOT outlive its node. The canonical usage is
// member-init aggregation — declare the node ref first and the endpoint handles after
// it, so reverse destruction retires the handles before the node. A moved-from handle
// is inert (null forwarder, empty retire); its destructor does nothing.
template <typename Policy>
    requires plexus::Policy<Policy>
class publisher
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
    // and is never reused (IDENT-02): a dropped publisher simply stops publishing. There
    // is no per-publisher resource to reclaim, so the destructor is trivial.
    ~publisher() = default;

private:
    io::message_forwarder<Policy> *m_messages{};
    std::string                    m_fqn;
};

}

#endif
