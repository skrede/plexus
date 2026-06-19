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

// The serving endpoint family. The codec slots are template-template parameters: a slot
// takes a codec FAMILY (a class template over one value type), not a finished codec, and
// the typed specialization applies CReq<request_t> / CRes<response_t>. The response family
// defaults to the request family (the symmetric form), so procedure<Res(Req), pair_codec>
// expands to pair_codec<request_t> / pair_codec<response_t>. The bytes endpoint is the
// procedure<void, no_codec, no_codec> specialization below — procedure<> selects it via the
// defaulted parameters (the defaults live in node.h's forward declaration, seen first), so
// every bytes spelling keeps compiling unchanged. no_codec is a sentinel family that names
// the bytes default and is never instantiated.
template<typename Sig, template<typename> class CReq, template<typename> class CRes>
class procedure;

// The bytes serving endpoint: the CONSTRUCTOR is the registration — it serves the
// handler on the node for the fqn — and the handle owns the served lifetime. The handler
// mirrors the procedure_forwarder's contract: it is invoked with the inbound request's
// opaque param bytes and a reply& it must invoke once with a wire::rpc_status and the
// opaque return bytes. The reply is a node-owned reused callable handed by reference (no
// per-dispatch allocation).
//
// DOUBLE-SERVE REFUSAL: a node REFUSES a second LOCAL registration on one fqn —
// the constructor throws std::logic_error and leaves the first handler serving (the
// forwarder's own serve() would silently overwrite; this facade gate closes the
// within-process hijack-by-overwrite). A constructor has no error-return channel, and a
// duplicate local provider is a programming error, so the throw is the contract.
//
// LIFETIME: a procedure must NOT outlive its node. The canonical usage is
// member-init aggregation (node ref first, handles after), so reverse destruction
// retires the handler before the node. Dropping the handle retires the handler: a
// subsequent inbound call for the fqn resolves rpc_status::no_handler (the existing
// absent-handler path), and the fqn is free to be served again. A moved-from handle is
// inert (empty retire); its destructor does nothing.
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
    std::string                                m_fqn;
    plexus::detail::move_only_function<void()> m_retire;
};

// The typed serving endpoint: an encode/decode adaptation around the bytes procedure.
// The codec slots are FAMILIES — CReq<Req> decodes the inbound request, CRes<Res> encodes
// a successful response — and each expansion must satisfy typed_codec (the requires-clause
// enforces it at the point of expansion, so a family that does not model a codec for a half
// names that half plainly). The handler is expected<Res, std::error_code>(const Req&).
// There is NO inproc fast path for RPC by design — a request/response always rides bytes.
//
// The registered bytes handler: decode the request via CReq<Req> (failure replies
// rpc_status::deserialize_failed with no Req ever handed to the handler — a decode
// failure never reaches the handler, the session stays up); invoke the handler; on
// success encode the Res via CRes<Res> and reply rpc_status::success;
// on a handler error encode the error_code's VALUE as a bounds-safe varint into the
// otherwise-empty error-leg payload and reply rpc_status::error (the typed caller
// reconstructs it under provider_category — value preserved, category erased).
//
// All the lifetime/double-serve guarantees of the bytes specialization hold verbatim:
// the ctor is the registration, a second LOCAL serve on one fqn throws std::logic_error
// with zero side effects, and dropping the handle retires it to rpc_status::no_handler.
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
    procedure(node<Policy, NodeTs...> &n, std::string_view fqn, Handler handler,
              request_codec req_codec = {}, response_codec res_codec = {})
            : m_fqn(fqn)
    {
        io::endpoint_seam seam = n.endpoint_seam_for();
        seam.serve_procedure(
                seam.ctx, fqn,
                handler_fn{adapt(std::move(handler), std::move(req_codec), std::move(res_codec))});
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
        return [handler = std::move(handler), req_codec = std::move(req_codec),
                res_codec = std::move(res_codec), scratch = std::vector<std::byte>{}](
                       std::span<const std::byte> param, reply_fn &reply) mutable
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

    std::string                                m_fqn;
    plexus::detail::move_only_function<void()> m_retire;
};

}

#endif
