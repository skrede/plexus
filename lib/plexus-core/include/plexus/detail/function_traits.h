#ifndef HPP_GUARD_PLEXUS_DETAIL_FUNCTION_TRAITS_H
#define HPP_GUARD_PLEXUS_DETAIL_FUNCTION_TRAITS_H

namespace plexus::detail {

// Decompose an RPC signature spelled as a function type Res(Req) into its halves
// via a function-type partial specialization (the canonical Boost.CallableTraits
// shape, not a dependency). The primary is undefined so a non-function-type Sig is
// an ill-formed use naming the request/response remedy, never a silent match.
template <typename Sig>
struct rpc_signature;

template <typename Res, typename Req>
struct rpc_signature<Res(Req)>
{
    using request  = Req;
    using response = Res;
};

template <typename Sig>
using request_of_t = typename rpc_signature<Sig>::request;

template <typename Sig>
using response_of_t = typename rpc_signature<Sig>::response;

}

#endif
