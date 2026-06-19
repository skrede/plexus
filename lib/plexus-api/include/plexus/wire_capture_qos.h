#ifndef HPP_GUARD_PLEXUS_WIRE_CAPTURE_QOS_H
#define HPP_GUARD_PLEXUS_WIRE_CAPTURE_QOS_H

namespace plexus {

// Where a captured frame is tapped relative to the AEAD seal. cleartext (the default)
// captures the framed bytes ABOVE the seal — the application/protocol-debuggable view a
// consumer wants first. ciphertext is the raw-wire forensic opt-in BELOW the seal (the
// sealed bytes as they cross the network).
enum class wire_crypto_position
{
    cleartext  = 0,
    ciphertext = 1,
};

// The construction-time per-transport wire-capture declaration: a plain designated-
// initializer aggregate naming whether a node's transport is captured at the wire tier and
// at which crypto position. It is the public sibling of recording_qos: a node carries it on
// node_options and the decorated-vs-bare channel TYPE is fixed where the channel is minted.
//
// enabled defaults to false — a wire_capture a consumer leaves alone SELECTS NOTHING, so a
// default node instantiates no decorator and pays zero (the structural-absence floor). The
// field is required-with-default (its absence is not meaningful, only its value is); it is
// never a std::optional standing in for a default.
struct wire_capture_qos
{
    bool                 enabled{false};
    wire_crypto_position position{wire_crypto_position::cleartext};
};

}

#endif
