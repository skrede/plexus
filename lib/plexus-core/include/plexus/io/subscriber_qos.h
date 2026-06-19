#ifndef HPP_GUARD_PLEXUS_IO_SUBSCRIBER_QOS_H
#define HPP_GUARD_PLEXUS_IO_SUBSCRIBER_QOS_H

#include "plexus/io/shm/dispatch_hint.h"

#include <cstdint>

namespace plexus::io {

// The subscriber-side replay choice for a latched topic. `none` declines every
// retained frame on subscribe; `latest` takes the single most-recent retained
// frame; `all` takes the retained history (capped at the ring depth). Wire bytes
// {0,1,2} pin each enumerator (append-only, never renumbered).
enum class durability : std::uint8_t
{
    none   = 0,
    latest = 1,
    all    = 2
};

// The subscriber-side delivery model. `push` lets the producer drive retained +
// live frames at the topic's pace; `pull` defers retained delivery to an explicit
// fetch. Wire bytes {0,1} pin each enumerator.
enum class delivery : std::uint8_t
{
    push = 0,
    pull = 1
};

// The subscriber-chosen handling of a SOFT request-vs-offered incompatibility
// (reliability, durability, deadline, lease). `permissive` (the friendly default)
// connects anyway but the producer surfaces which soft fields went unsatisfied — a
// degraded-accept, never a silent one. `strict` refuses such a pair with a reason.
// `requires_source_identity` is unaffected: it is the one always-hard field that
// refuses regardless of this choice.
enum class rxo_mode : std::uint8_t
{
    permissive = 0,
    strict     = 1
};

// The subscriber-chosen handling of attaching to a producer that declared no type.
// `lenient` (the friendly default) attaches to an untyped producer and delivers its
// frames as bytes; `strict` refuses such an attach with a distinct verdict so a typed
// consumer never silently binds to a producer whose type it cannot gate. Orthogonal to
// rxo_mode (which governs the SOFT QoS-field relations): this is the typed-family gate.
enum class attach_posture : std::uint8_t
{
    lenient = 0,
    strict  = 1
};

// The subscriber-CHOICE QoS value struct: a different actor's decision than the
// publisher-declared topic_qos, carried on its own wire path (the subscribe
// request) and stored once per subscriber at attach. Plain fields with defaults,
// trivially copyable; plexus has no QoS negotiation, so each "unset" value is a
// genuine, tested choice — never a euphemism for an unimplemented feature.
//
// The defaults are the maximally-accepting required-with-default resolution: a
// subscriber that carries no choice genuinely chose `durability=none` (accept
// whatever the topic offers — an unstated request never silently DEMANDS retention,
// so a plain subscribe is always compatible) and `delivery=push`. A consumer wanting
// late-join replay opts into `latest`/`all` explicitly; an explicit request the
// producer cannot satisfy is then honestly surfaced (degraded) or refused per
// `rxo`, never silently dropped. `replay_depth=0` means "use the ring
// depth" for an `all` replay, not "replay nothing". The reserved request-side
// fields below are carried on the wire NOW and become enforcement inputs in later
// work WITHOUT a further protocol bump: `requires_source_identity=false` and
// `requested_reliability_reliable=false` are genuine "not requested" states; each
// `*_ns=0` is "no deadline / lease requested" (absence, not zero-duration); and
// `requested_priority=0` is the default band. No std::optional is used — the
// 0-sentinel and the enum default ARE the meaningful, tested absence.
struct subscriber_qos
{
    durability    durability_mode = durability::none;
    delivery      delivery_mode   = delivery::push;
    std::uint32_t replay_depth    = 0;

    bool          requires_source_identity       = false;
    bool          requested_reliability_reliable = false;
    std::uint64_t requested_deadline_ns          = 0;
    std::uint64_t requested_lease_ns             = 0;
    std::uint8_t  requested_priority             = 0;

    // The subscriber's requested per-MESSAGE size ceiling: the largest message it will
    // accept. 0 = unset = always compatible (a genuine "not requested" state, the same
    // 0-sentinel semantics as the deadline/lease fields — never a std::optional). It rides
    // the subscribe wire region and gates the RxO max-message-bytes relation: a publisher
    // whose effective-max exceeds this is refused (strict) or degraded (permissive).
    std::uint32_t requested_max_message_bytes = 0;

    // The subscriber-chosen handling of a soft RxO incompatibility. `permissive`
    // (the friendly default) connects-but-surfaces the degraded set; `strict`
    // refuses with a reason. It rides a reserved subscribe-request flag bit, so a
    // permissive default keeps the wire encoding byte-identical (the bit is clear).
    rxo_mode rxo = rxo_mode::permissive;

    // The subscriber-chosen typed attach posture. `lenient` (the friendly default)
    // attaches to an untyped producer; `strict` refuses it with type_undeclared. It
    // rides a reserved subscribe-request flag bit, so the lenient default keeps the wire
    // encoding byte-identical (the bit is clear).
    attach_posture posture = attach_posture::lenient;

    // The local-only inproc stamp demand: true (default) means this subscriber consumes
    // message_info timestamps; false elides the receive-side clock read and delivers a
    // documented 0 stamp. Set implicitly from the callback arity (a 2-arg subscriber wants
    // no info) and overridable on a 3-arg subscriber. NEVER crosses a socket: an inproc
    // subscriber is local by definition, so this field is not encoded into the subscribe
    // wire region (a cross-process demand leak is forbidden).
    bool wants_message_info = true;

    // The subscriber's own shared-memory eligibility hint: a same-host subscriber with
    // any bit set rescues itself from a hint-less publisher (the bilateral OR — EITHER
    // end's hint upgrades the pair). none = 0 is the absence (stay on the local stream).
    // Local-only, never encoded into the subscribe wire region (the upgrade converges on
    // local state with no wire exchange).
    shm::dispatch_hint dispatch = shm::dispatch_hint::none;

    // A defaulted value-equality so a caller can ask "does this differ from the
    // friendly default?" before paying the wire region — the subscribe-out path
    // keeps the encode byte-identical when the choice IS the default.
    friend bool operator==(const subscriber_qos &, const subscriber_qos &) = default;
};

}

#endif
