#ifndef HPP_GUARD_PLEXUS_WIRE_CAPTURE_QOS_H
#define HPP_GUARD_PLEXUS_WIRE_CAPTURE_QOS_H

namespace plexus {

// Where a captured frame is tapped relative to the AEAD seal: cleartext above it (the framed
// bytes), ciphertext below it (the sealed bytes as they cross the network).
enum class wire_crypto_position
{
    cleartext  = 0,
    ciphertext = 1,
};

struct wire_capture_qos
{
    bool enabled{false};
    wire_crypto_position position{wire_crypto_position::cleartext};
};

}

#endif
