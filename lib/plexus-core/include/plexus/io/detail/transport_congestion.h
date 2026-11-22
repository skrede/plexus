#ifndef HPP_GUARD_PLEXUS_IO_DETAIL_TRANSPORT_CONGESTION_H
#define HPP_GUARD_PLEXUS_IO_DETAIL_TRANSPORT_CONGESTION_H

#include "plexus/io/congestion.h"

#include "plexus/detail/fail_closed.h"

namespace plexus::io::detail {

// A transport's bounded queue can only shed what arrives or push back, so drop_oldest is refused here rather than
// silently run as block.
inline io::congestion checked_transport_congestion(io::congestion c)
{
    if(c == io::congestion::drop_oldest)
        plexus::detail::fail_closed("a transport sheds with drop_newest or blocks; drop_oldest is a topic policy");
    return c;
}

}

#endif
