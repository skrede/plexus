#ifndef HPP_GUARD_PLEXUS_FREERTOS_DETAIL_LWIP_CHANNEL_INBOUND_H
#define HPP_GUARD_PLEXUS_FREERTOS_DETAIL_LWIP_CHANNEL_INBOUND_H

#include "plexus/wire/frame.h"
#include "plexus/wire/close_cause.h"

#include "plexus/detail/compat.h"

#include <span>
#include <cstddef>

namespace plexus::freertos::detail {

using on_data_cb           = plexus::detail::move_only_function<void(std::span<const std::byte>)>;
using on_protocol_close_cb = plexus::detail::move_only_function<void(plexus::wire::close_cause)>;

// Wire the reused stream_inbound's two seams to the channel's callbacks (borrowed by reference, they
// outlive this wiring). A complete frame delivers SYNCHRONOUSLY: poll() runs on the executor task and
// consumes the framed span FULLY before returning, so a synchronous on_data re-enters nothing the loop
// has not finished (the re-entrancy invariant holds). This deliberately avoids the per-frame owning
// heap copy a posted delivery needs, which is banned on the -fno-exceptions target — do NOT "restore"
// the post. A framing violation fires on_protocol_close, the seam DISTINCT from on_error.
template<typename Inbound>
void lwip_wire_inbound(Inbound &inbound, on_data_cb &on_data, on_protocol_close_cb &on_protocol_close)
{
    inbound.on_frame(
            [&on_data](const plexus::wire::complete_frame &f)
            {
                if(on_data)
                    on_data(static_cast<std::span<const std::byte>>(f.payload));
            });
    inbound.on_protocol_close(
            [&on_protocol_close](plexus::wire::close_cause cause)
            {
                if(on_protocol_close)
                    on_protocol_close(cause);
            });
}

}

#endif
