#ifndef HPP_GUARD_PLEXUS_RECORDING_HOST_ROTATING_SINK_H
#define HPP_GUARD_PLEXUS_RECORDING_HOST_ROTATING_SINK_H

#include "plexus/recording/host/file_sink.h"

#include "plexus/io/recording/byte_sink.h"

#include <span>
#include <chrono>
#include <memory>
#include <string>
#include <vector>
#include <cstddef>
#include <utility>
#include <optional>
#include <algorithm>
#include <filesystem>

namespace plexus::recording::host {

// A byte_sink that spreads a capture over rotating segment files. Rotation is opt-in by segment
// size and/or elapsed time with keep-N retention; segments are named from consumer config alone
// (basename + zero-padded index), never from capture content. Each NEW segment is prefixed with
// the recorder's cached head+defs preamble (prime_preamble), so every rotated segment carries the
// stream definitions and decodes standalone. byte_sink stays opaque: the sink treats the preamble
// as bytes and learns nothing of the stream format.
class rotating_sink final : public plexus::io::recording::byte_sink
{
public:
    struct options
    {
        std::filesystem::path directory{};
        std::string basename{"capture"};
        std::string extension{".plxr"};
        std::optional<std::size_t> max_bytes{};
        std::optional<std::chrono::steady_clock::duration> max_interval{};
        std::optional<std::size_t> keep{};
    };

    explicit rotating_sink(options opts)
            : m_opts(std::move(opts))
            , m_segments()
            , m_active()
            , m_preamble()
            , m_segment_bytes(0)
            , m_index(0)
            , m_opened_at(std::chrono::steady_clock::now())
    {
        open_segment();
    }

    void write(std::span<const std::byte> bytes) override
    {
        if(should_rotate(bytes.size()))
            rotate();
        m_active->write(bytes);
        m_segment_bytes += bytes.size();
    }

    void flush() override
    {
        if(m_active)
            m_active->flush();
    }

    // The recorder's cached head+defs blob, re-emitted at the head of each new segment. The first
    // segment already carries it (the recorder writes it as the opening write); priming supplies it
    // for every rotation that follows.
    void prime_preamble(std::span<const std::byte> preamble)
    {
        m_preamble.assign(preamble.begin(), preamble.end());
    }

    // Close the active segment and open the next, re-emitting the primed preamble. Size/time
    // thresholds drive rotation automatically; a caller forces a segment boundary here.
    void rotate()
    {
        open_segment();
        if(!m_preamble.empty())
        {
            m_active->write(m_preamble);
            m_segment_bytes += m_preamble.size();
        }
    }

    std::span<const std::filesystem::path> segments() const noexcept
    {
        return m_segments;
    }

private:
    bool should_rotate(std::size_t incoming) const
    {
        if(m_segment_bytes == 0)
            return false;
        if(m_opts.max_bytes && m_segment_bytes + incoming > *m_opts.max_bytes)
            return true;
        return m_opts.max_interval && std::chrono::steady_clock::now() - m_opened_at >= *m_opts.max_interval;
    }

    void open_segment()
    {
        m_active.reset();
        m_active        = std::make_unique<file_sink>(next_path());
        m_segment_bytes = 0;
        m_opened_at     = std::chrono::steady_clock::now();
        enforce_retention();
    }

    std::filesystem::path next_path()
    {
        std::string index = std::to_string(m_index++);
        index.insert(0, k_index_width - std::min(k_index_width, index.size()), '0');
        std::filesystem::path segment = m_opts.basename + "_" + index + m_opts.extension;
        std::filesystem::path full    = m_opts.directory.empty() ? segment : m_opts.directory / segment;
        m_segments.push_back(full);
        return full;
    }

    void enforce_retention()
    {
        if(!m_opts.keep)
            return;
        while(m_segments.size() > *m_opts.keep)
        {
            std::error_code ec;
            std::filesystem::remove(m_segments.front(), ec);
            m_segments.erase(m_segments.begin());
        }
    }

    static constexpr std::size_t k_index_width = 6;

    options m_opts;
    std::vector<std::filesystem::path> m_segments;
    std::unique_ptr<file_sink> m_active;
    std::vector<std::byte> m_preamble;
    std::size_t m_segment_bytes;
    std::size_t m_index;
    std::chrono::steady_clock::time_point m_opened_at;
};

}

#endif
