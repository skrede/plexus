#ifndef HPP_GUARD_PLEXUS_PROCEDURE_H
#define HPP_GUARD_PLEXUS_PROCEDURE_H

#include "plexus/node.h"
#include "plexus/expected.h"
#include "plexus/typed_codec.h"

#include "plexus/io/endpoint_seam.h"

#include "plexus/wire/varint.h"
#include "plexus/wire/rpc_status.h"

#include "plexus/detail/compat.h"

#include <span>
#include <string>
#include <vector>
#include <cstddef>
#include <utility>
#include <system_error>
#include <string_view>

namespace plexus {

template<typename Sig, template<typename> class CReq, template<typename> class CRes>
class procedure;

// The handler is invoked with the inbound request's opaque param bytes and a reply& it must
// invoke once; the reply is a node-owned reused callable handed by reference (no per-dispatch
// allocation).
//
// DOUBLE-SERVE REFUSAL: a second LOCAL registration on one fqn throws (a ctor has no
// error-return channel, and a duplicate local provider is a programming error, so the throw is
// the contract) and leaves the first handler serving, closing the forwarder's silent overwrite.
//
// LIFETIME: a procedure must NOT outlive its node. Dropping the handle retires the handler: a
// subsequent inbound call for the fqn resolves rpc_status::no_handler.
template<>
class procedure<void, no_codec, no_codec>
{
public:
    using reply_fn   = io::reply_fn;
    using handler_fn = io::handler_fn;

    template<typename Policy, typename... NodeTs, typename Handler>
    procedure(node<Policy, NodeTs...> &n, std::string_view fqn, Handler handler)
            : m_fqn(fqn)
    {
        io::endpoint_seam seam = n.endpoint_seam_for();
        seam.serve_procedure(seam.ctx, fqn, handler_fn{std::move(handler)});
        m_retire = [seam, fqn = m_fqn] { seam.retire_procedure(seam.ctx, fqn); };
    }

    procedure(procedure &&) noexcept            = default;
    procedure &operator=(procedure &&) noexcept = default;

    procedure(const procedure &)            = delete;
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

// An encode/decode adaptation around the bytes procedure. There is NO inproc fast path for RPC
// by design — a request/response always rides bytes.
//
// The registered bytes handler decodes the request via CReq<Req> (failure replies
// deserialize_failed with no Req ever handed to the handler, the session stays up), invokes the
// handler, and on success encodes the Res via CRes<Res>; on a handler error it encodes the
// error_code's VALUE as a bounds-safe varint into the error-leg payload (the typed caller
// reconstructs it under provider_category — value preserved, category erased).
template<typename Res, typename Req, template<typename> class CReq, template<typename> class CRes>
    requires typed_codec<CReq<Req>> && typed_codec<CRes<Res>>
class procedure<Res(Req), CReq, CRes>
{
public:
    using reply_fn       = io::reply_fn;
    using handler_fn     = io::handler_fn;
    using request_codec  = CReq<Req>;
    using response_codec = CRes<Res>;

    template<typename Policy, typename... NodeTs, typename Handler>
    procedure(node<Policy, NodeTs...> &n, std::string_view fqn, Handler handler, request_codec req_codec = {}, response_codec res_codec = {})
            : m_fqn(fqn)
    {
        io::endpoint_seam seam = n.endpoint_seam_for();
        seam.serve_procedure(seam.ctx, fqn, handler_fn{adapt(std::move(handler), std::move(req_codec), std::move(res_codec))});
        m_retire = [seam, fqn = m_fqn] { seam.retire_procedure(seam.ctx, fqn); };
    }

    procedure(procedure &&) noexcept            = default;
    procedure &operator=(procedure &&) noexcept = default;

    procedure(const procedure &)            = delete;
    procedure &operator=(const procedure &) = delete;

    ~procedure()
    {
        if(m_retire != nullptr)
            m_retire();
    }

private:
    template<typename Handler>
    static handler_fn adapt(Handler handler, request_codec req_codec, response_codec res_codec)
    {
        return [handler = std::move(handler), req_codec = std::move(req_codec), res_codec = std::move(res_codec), scratch = std::vector<std::byte>{}](std::span<const std::byte> param,
                                                                                                                                                      reply_fn &reply) mutable
        {
            Req request{};
            if(!req_codec.decode(param, request))
            {
                reply(wire::rpc_status::deserialize_failed, {});
                return;
            }
            expected<Res, std::error_code> result = handler(request);
            if(result)
            {
                const wire_bytes<> encoded = res_codec.encode(result.value());
                reply(wire::rpc_status::success, static_cast<std::span<const std::byte>>(encoded));
                return;
            }
            scratch.clear();
            wire::write_varint(scratch, static_cast<std::uint64_t>(result.error().value()));
            reply(wire::rpc_status::error, scratch);
        };
    }

    std::string m_fqn;
    plexus::detail::move_only_function<void()> m_retire;
};

}

#endif
