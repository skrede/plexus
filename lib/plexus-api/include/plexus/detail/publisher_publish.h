#ifndef HPP_GUARD_PLEXUS_DETAIL_PUBLISHER_PUBLISH_H
#define HPP_GUARD_PLEXUS_DETAIL_PUBLISHER_PUBLISH_H

#include "plexus/detail/loan_pool.h"

#include "plexus/io/endpoint_seam.h"
#include "plexus/io/object_carrier.h"

#include <span>
#include <string>
#include <cstddef>

namespace plexus::detail {

// Gate on the moved-from sentinel, then swallow any exception the posted edge raises — a dtor must
// not throw.
inline void retire_publisher_quiet(const io::endpoint_seam &seam, const std::string &fqn) noexcept
{
    if(seam.ctx == nullptr)
        return;
#if defined(__cpp_exceptions)
    try
    {
        seam.retire_publisher(seam.ctx, fqn);
    }
    catch(...)
    {
    }
#else
    seam.retire_publisher(seam.ctx, fqn);
#endif
}

// Encode one value into the publisher's reused scratch and return a span into it — valid for the
// synchronous publish verb's duration only.
template<typename Publisher, typename ValueType>
std::span<const std::byte> encode_to_span(Publisher &pub, const ValueType &value)
{
    pub.m_scratch = pub.m_codec.encode(value);
    return static_cast<std::span<const std::byte>>(pub.m_scratch);
}

// A valid loan rides the process-tier lane by address — true zero-copy, encode never invoked
// in-process; an empty loan is a no-op.
template<typename Publisher, typename ValueType>
void publish_loan(Publisher &pub, loan<ValueType> &&held)
{
    if(!held)
        return;
    auto carrier           = loan_pool<ValueType>::carrier_for(held, pub.m_identity.type_id);
    const ValueType &value = *static_cast<const ValueType *>(carrier.slot->object);
    auto encode            = [&] { return encode_to_span(pub, value); };
    pub.m_seam.publish_object(pub.m_seam.ctx, pub.m_fqn, carrier, io::make_encode_thunk(encode));
}

// On pool exhaustion, serialize directly and publish bytes — a counted graceful degradation, never
// a block, never an allocation.
template<typename Publisher, typename ValueType>
void publish_value(Publisher &pub, const ValueType &value)
{
    auto held = pub.m_pool.try_borrow(value);
    if(held)
    {
        publish_loan(pub, std::move(held));
        return;
    }
    ++pub.m_loan_exhausted;
    pub.m_seam.publish(pub.m_seam.ctx, pub.m_fqn, encode_to_span(pub, value));
}

}

#endif
