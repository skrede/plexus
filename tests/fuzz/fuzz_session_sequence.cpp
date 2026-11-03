#include "fuzz_clock.h"
#include "fuzz_input.h"
#include "fuzz_oracle.h"
#include "fuzz_session_link.h"
#include "fuzz_session_frames.h"

#include <span>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>

namespace {

using namespace fuzz_session;

constexpr std::size_t k_max_operations = 64;

side side_field(fuzz_input &in)
{
    return in.u8() % 2 == 0 ? side::requester : side::responder;
}

void require_quiescence(session_link &link)
{
    fuzz_require(link.drive(), "the executor reaches quiescence within the step bound");
}

frame encoded_frame(fuzz_input &in)
{
    switch(in.u8() % 5)
    {
        case 0:
            return handshake_frame(in);
        case 1:
            return heartbeat_frame(in);
        case 2:
            return subscribe_frame(in);
        case 3:
            return data_frame(in);
        default:
            return declaration_frame(in);
    }
}

// The link is settled first, so nothing the peer sent earlier can close the session during the following drive,
// and the injected frame is the only new input on either side.
void inject_unknown(session_link &link, fuzz_input &in)
{
    const side s  = side_field(in);
    const frame f = unknown_frame(in);
    require_quiescence(link);
    const bool open = !link.at(s).closed();
    link.inject(s, f);
    fuzz_require(!open || !link.at(s).closed(), "an unknown msg_type does not close the session it reaches");
    require_quiescence(link);
    fuzz_require(!open || !link.at(s).closed(), "an unknown msg_type does not close the session it reaches, after a drive");
}

void inject_frame(session_link &link, fuzz_input &in)
{
    const side s  = side_field(in);
    const frame f = encoded_frame(in);
    link.inject(s, f);
}

void inject_raw(session_link &link, fuzz_input &in)
{
    const side s = side_field(in);
    link.inject(s, std::as_bytes(in.take(in.u8())));
}

// Settled first, as for an unknown type. Before an epoch is latched the first non-zero session_id becomes it, so the
// oracle applies only once one is.
void inject_foreign(session_link &link, fuzz_input &in)
{
    const side s = side_field(in);
    require_quiescence(link);
    session_end &end       = link.at(s);
    const bool established = end.live && end.live->is_complete() && !end.closed() && end.live->peer_session_id() != 0;
    const frame f          = foreign_frame(in, established ? end.live->peer_session_id() : 0);
    link.inject(s, f);
    require_quiescence(link);
    fuzz_require(!established || !end.closed(), "a frame from a foreign epoch never closes an established session");
}

void subscribe(session_link &link, fuzz_input &in)
{
    const side s = side_field(in);
    link.call(s, [fqn = text_field(in)](session &live) { live.subscribe(fqn); });
}

void declare(session_link &link, fuzz_input &in)
{
    const side s = side_field(in);
    link.call(s, [td = declaration_field(in)](session &live) { live.declare(td); });
}

void advance(session_link &link, fuzz_input &in)
{
    fuzz_clock::advance(std::chrono::milliseconds{static_cast<std::int64_t>(in.uint(2) % 4096)});
    require_quiescence(link);
}

void drive(session_link &link, fuzz_input &)
{
    require_quiescence(link);
}

void heartbeat(session_link &link, fuzz_input &in)
{
    link.call(side_field(in), [](session &s) { s.emit_heartbeat(); });
}

void tear_down(session_link &link, fuzz_input &in)
{
    link.tear_down(side_field(in));
}

using operation = void (*)(session_link &, fuzz_input &);

// The index is the operation byte the seeds encode.
constexpr std::array<operation, 10> k_operations{drive, inject_frame, inject_raw, inject_unknown, subscribe, declare, heartbeat, tear_down, advance, inject_foreign};

void apply(session_link &link, fuzz_input &in)
{
    k_operations[in.u8() % k_operations.size()](link, in);
}

void check(session_end &end)
{
    if(!end.live)
        return;
    const bool closed        = end.closed();
    const std::uint64_t peer = end.live->peer_session_id();
    fuzz_require(!end.seen.complete || end.live->is_complete() || closed, "completion never reverts while the session is open");
    fuzz_require(end.seen.peer_session_id == 0 || peer == end.seen.peer_session_id || (closed && peer == 0), "a latched peer_session_id never changes");
    fuzz_require(!end.seen.identity || closed || end.live->peer_identity() == *end.seen.identity, "a proven identity never changes while the session is open");
    fuzz_require(end.seen.disconnects == 0 || (end.seen.disconnects == 1 && closed), "a disconnected edge fires at most once, from a closed session");
    end.seen.complete = end.seen.complete || end.live->is_complete();
    if(end.seen.peer_session_id == 0)
        end.seen.peer_session_id = peer;
}

void finish(session_link &link)
{
    link.tear_down(side::requester);
    link.tear_down(side::responder);
    require_quiescence(link);
    for(const side s : {side::requester, side::responder})
    {
        auto &end = link.at(s);
        check(end);
        fuzz_require(end.live->pending().count() == 0 && end.live->pending().settled(), "after both teardowns and a drain nothing is pending");
    }
}

}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    fuzz_clock::reset();
    fuzz_input in{std::span<const std::uint8_t>{data, size}};
    session_link link;
    for(std::size_t n = 0; n < k_max_operations && !in.exhausted(); ++n)
    {
        apply(link, in);
        check(link.at(side::requester));
        check(link.at(side::responder));
    }
    finish(link);
    return 0;
}
