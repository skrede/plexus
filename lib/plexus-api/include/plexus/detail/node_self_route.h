#ifndef HPP_GUARD_PLEXUS_DETAIL_NODE_SELF_ROUTE_H
#define HPP_GUARD_PLEXUS_DETAIL_NODE_SELF_ROUTE_H

#include "plexus/io/polymorphic_byte_channel.h"
#include "plexus/io/reference_channel_adapter.h"

#include <memory>
#include <utility>
#include <type_traits>

namespace plexus::detail {

// The zero-size stand-in a single-transport node carries in place of the erased self-route wrapper:
// the concrete loopback channel IS the engine channel there, so no wrapper is needed.
struct no_self_route_wrapper
{
};

// Build the self-route holder from the node-owned concrete channel. The single-transport case wants
// nothing (the monostate); the multi-transport case erases the concrete behind a non-owning adapter
// so the registry's reference_wrapper<polymorphic_byte_channel> binds to a stable node-owned object.
template<typename Holder, typename Concrete>
Holder make_self_route(Concrete &concrete)
{
    if constexpr(std::is_same_v<Holder, no_self_route_wrapper>)
        return Holder{};
    else
        return Holder{std::make_unique<io::reference_channel_adapter<Concrete>>(concrete)};
}

}

#endif
