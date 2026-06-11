#ifndef HPP_GUARD_PLEXUS_PROCEDURE_H
#define HPP_GUARD_PLEXUS_PROCEDURE_H

#include "plexus/node.h"

#include "plexus/io/procedure_forwarder.h"

#include "plexus/wire/rpc_status.h"

#include "plexus/policy.h"

#include "plexus/detail/compat.h"

#include <span>
#include <string>
#include <cstddef>
#include <utility>
#include <string_view>

namespace plexus {

// A move-only RAII serving endpoint (D-10/D-11): the CONSTRUCTOR is the registration —
// it serves the handler on the node for the fqn — and the handle owns the served
// lifetime. The handler mirrors the procedure_forwarder's contract: it is invoked with
// the inbound request's opaque param bytes and a reply& it must invoke once with a
// wire::rpc_status and the opaque return bytes. The reply is a node-owned reused
// callable handed by reference (no per-dispatch allocation). Templated on Policy alone:
// the handler/reply signatures carry no Policy-dependent type, so one procedure type
// serves a node over any transport pack.
//
// DOUBLE-SERVE REFUSAL (D-03): a node REFUSES a second LOCAL registration on one fqn —
// the constructor throws std::logic_error and leaves the first handler serving (the
// forwarder's own serve() would silently overwrite; this facade gate closes the
// within-process hijack-by-overwrite). A constructor has no error-return channel, and a
// duplicate local provider is a programming error, so the throw is the contract.
//
// LIFETIME (D-13): a procedure must NOT outlive its node. The canonical usage is
// member-init aggregation (node ref first, handles after), so reverse destruction
// retires the handler before the node. Dropping the handle retires the handler: a
// subsequent inbound call for the fqn resolves rpc_status::no_handler (the existing
// absent-handler path), and the fqn is free to be served again. A moved-from handle is
// inert (empty retire); its destructor does nothing.
template <typename Policy>
    requires plexus::Policy<Policy>
class procedure
{
public:
    using reply_fn = typename io::procedure_forwarder<Policy>::reply_fn;
    using handler_fn = typename io::procedure_forwarder<Policy>::handler_fn;

    template <typename... NodeTs, typename Handler>
    procedure(node<Policy, NodeTs...> &n, std::string_view fqn, Handler handler)
        : m_fqn(fqn)
    {
        n.serve_procedure_seam(fqn, handler_fn{std::move(handler)});
        m_retire = [&n, fqn = m_fqn] { n.retire_procedure_seam(fqn); };
    }

    procedure(procedure &&) noexcept = default;
    procedure &operator=(procedure &&) noexcept = default;

    procedure(const procedure &) = delete;
    procedure &operator=(const procedure &) = delete;

    ~procedure()
    {
        if(m_retire != nullptr)
            m_retire();
    }

private:
    std::string m_fqn;
    plexus::detail::move_only_function<void()> m_retire;
};

}

#endif
