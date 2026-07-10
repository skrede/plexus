#ifndef HPP_GUARD_PLEXUS_RECORDING_HOST_FILE_SINK_H
#define HPP_GUARD_PLEXUS_RECORDING_HOST_FILE_SINK_H

#include "plexus/io/recording/byte_sink.h"

#include <span>
#include <fstream>
#include <cstddef>
#include <filesystem>

namespace plexus::recording::host {

// The drop-in host drain a consumer attaches to a recorder instead of hand-writing byte_sink::write:
// a binary std::ofstream opened truncating. It depends on std::filesystem/std::ofstream, so it lives
// in this host-only lib and never reaches the core/MCU build the portable byte_sink ABC serves.
class file_sink final : public plexus::io::recording::byte_sink
{
public:
    explicit file_sink(const std::filesystem::path &path)
            : m_out(path, std::ios::binary | std::ios::trunc)
    {
    }

    void write(std::span<const std::byte> bytes) override
    {
        m_out.write(reinterpret_cast<const char *>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    }

    void flush() override
    {
        m_out.flush();
    }

private:
    std::ofstream m_out;
};

}

#endif
