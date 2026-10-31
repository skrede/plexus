#include "fuzz_clock.h"
#include "fuzz_input.h"
#include "fuzz_oracle.h"

#include "plexus/stream/stream_inbound.h"

#include "plexus/inproc/inproc_bus.h"
#include "plexus/inproc/inproc_timer.h"
#include "plexus/inproc/inproc_channel.h"
#include "plexus/inproc/inproc_executor.h"

#include "plexus/wire/frame.h"
#include "plexus/wire/close_cause.h"
#include "plexus/wire/frame_reassembler.h"

#include <span>
#include <chrono>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <algorithm>

namespace {

using namespace plexus;

using fuzz_bus      = inproc::inproc_bus<fuzz_clock>;
using fuzz_timer    = inproc::inproc_timer<fuzz_clock>;
using fuzz_executor = inproc::inproc_executor<fuzz_clock>;
using fuzz_stream   = stream::stream_inbound<fuzz_timer, fuzz_executor &>;

stream::stream_inbound_config read_config(fuzz_input &in)
{
    return stream::stream_inbound_config{.no_progress_floor            = std::chrono::milliseconds{1 + in.u8()},
                                         .min_throughput_bytes_per_sec = static_cast<std::size_t>(in.uint(2)),
                                         .max_payload_size             = static_cast<std::size_t>(in.uint(2) % 4096),
                                         .buffered_bytes_cap           = wire::header_size + static_cast<std::size_t>(in.uint(2) % 8192)};
}

class stream_rig
{
public:
    explicit stream_rig(fuzz_input &in)
            : m_closed(false)
            , m_bus()
            , m_ex(m_bus)
            , m_stream(m_ex, read_config(in))
            , m_fed()
            , m_delivered()
    {
        m_stream.on_frame([this](const wire::complete_frame &frame) { on_frame(frame); });
        m_stream.on_protocol_close([this](wire::close_cause cause) { on_close(cause); });
    }

    void step(fuzz_input &in)
    {
        switch(in.u8() % 3)
        {
            case 0:
                feed(in.take(in.u8()));
                break;
            case 1:
                m_ex.drain();
                break;
            default:
                fuzz_clock::advance(std::chrono::milliseconds{static_cast<std::int64_t>(in.uint(2))});
                m_ex.drain();
                break;
        }
    }

    void finish()
    {
        m_stream.shutdown();
        m_ex.drain();
        fuzz_require(m_stream.pending().count() == 0 && m_stream.pending().settled(), "after shutdown and a drain nothing is pending");
    }

private:
    bool m_closed;
    fuzz_bus m_bus;
    fuzz_executor m_ex;
    fuzz_stream m_stream;
    std::vector<std::byte> m_fed;
    std::vector<std::byte> m_delivered;

    // The owning channel closes on a protocol close and reads nothing more.
    void feed(std::span<const std::uint8_t> chunk)
    {
        if(m_closed)
            return;
        const auto bytes = std::as_bytes(chunk);
        m_fed.insert(m_fed.end(), bytes.begin(), bytes.end());
        m_stream.feed(bytes);
    }

    void on_frame(const wire::complete_frame &frame)
    {
        fuzz_require(!m_closed, "no frame is delivered after a protocol close");
        fuzz_require(frame.payload.size() == wire::header_size + frame.header.payload_len, "a complete frame's declared payload_len equals its payload");
        const std::size_t at = m_delivered.size();
        m_delivered.insert(m_delivered.end(), frame.payload.begin(), frame.payload.end());
        fuzz_require(m_delivered.size() <= m_fed.size() && std::equal(m_delivered.begin() + at, m_delivered.end(), m_fed.begin() + at),
                     "the delivered frames, concatenated, are a prefix of the bytes fed");
    }

    void on_close(wire::close_cause cause)
    {
        fuzz_require(!m_closed, "on_protocol_close fires at most once");
        fuzz_require(cause != wire::close_cause::no_progress_timeout || m_fed.size() > m_delivered.size(), "a no-progress close needs a frame in progress");
        m_closed = true;
    }
};

}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    fuzz_clock::reset();
    fuzz_input in{std::span<const std::uint8_t>{data, size}};
    stream_rig rig{in};
    while(!in.exhausted())
        rig.step(in);
    rig.finish();
    return 0;
}
