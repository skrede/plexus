#ifndef HPP_GUARD_PLEXUS_IO_RECORDING_BYTE_SINK_H
#define HPP_GUARD_PLEXUS_IO_RECORDING_BYTE_SINK_H

#include <span>
#include <cstddef>

namespace plexus::io::recording {

class byte_sink
{
public:
    virtual ~byte_sink() = default;

    virtual void write(std::span<const std::byte> bytes) = 0;
    virtual void flush()
    {
    }
};

}

#endif
