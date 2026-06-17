#ifndef HPP_GUARD_PLEXUS_IO_RECORDING_BYTE_SINK_H
#define HPP_GUARD_PLEXUS_IO_RECORDING_BYTE_SINK_H

#include <span>
#include <cstddef>

namespace plexus::io::recording {

// Where drained capture bytes land. The recorder owns the one canonical flat
// encoding and hands the encoded bytes here; the sink chooses only the
// DESTINATION (an in-memory buffer in tests, a serial/SD/file binding on the
// MCU port), never the format — so it is a raw-byte drain, never a codec. A
// buffering destination overrides flush() to commit; the default is a no-op.
class byte_sink
{
public:
    virtual ~byte_sink() = default;

    virtual void write(std::span<const std::byte> bytes) = 0;
    virtual void flush() {}
};

}

#endif
