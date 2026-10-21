#ifndef HPP_GUARD_PLEXUS_FREERTOS_DETAIL_LWIP_CHANNEL_INBOUND_H
#define HPP_GUARD_PLEXUS_FREERTOS_DETAIL_LWIP_CHANNEL_INBOUND_H

#include "plexus/wire/frame.h"
#include "plexus/wire/close_cause.h"

#include "plexus/detail/compat.h"

#include <span>
#include <cstddef>
#include <utility>

namespace plexus::freertos::detail {

using on_data_cb           = plexus::detail::move_only_function<void(std::span<const std::byte>)>;
using on_protocol_close_cb = plexus::detail::move_only_function<void(plexus::wire::close_cause)>;

// Wire the reused stream_inbound's two seams to the channel's callbacks (borrowed by reference, they
// outlive this wiring). A complete frame delivers SYNCHRONOUSLY: poll() runs on the executor task and
// consumes the framed span FULLY before returning, so a synchronous on_data re-enters nothing the loop
// has not finished (the re-entrancy invariant holds). This deliberately avoids the per-frame owning
// heap copy a posted delivery needs, which is banned on the -fno-exceptions target — do NOT "restore"
// the post. A framing violation closes the channel, then fires the on_protocol_close it took first,
// the seam DISTINCT from on_error.
template<typename Inbound, typename Close>
void lwip_wire_inbound(Inbound &inbound, on_data_cb &on_data, on_protocol_close_cb &on_protocol_close, Close close)
{
    inbound.on_frame(
            [&on_data](const plexus::wire::complete_frame &f)
            {
                if(on_data)
                    on_data(static_cast<std::span<const std::byte>>(f.payload));
            });
    inbound.on_protocol_close(
            [&on_protocol_close, close = std::move(close)](plexus::wire::close_cause cause) mutable
            {
                auto handler = std::exchange(on_protocol_close, nullptr);
                close();
                if(handler)
                    handler(cause);
            });
}

}

#endif
